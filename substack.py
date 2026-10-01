"""Podcast feed of a Substack publication, using the text-to-speech audio Substack generates for its app.

Substack's undocumented archive API lists every post with its TTS MP3 on S3, or for podcast posts the
episode's MP3. The feed links those files directly. Free posts still without audio a day after
publishing are saved as articles for Gemini to read. Each publication's posts are saved as JSON, and
a refresh fetches only archive pages newer than the saved posts.
"""

import asyncio
import json
import logging
import time
from calendar import timegm
from datetime import datetime

import feedparser
from starlette.exceptions import HTTPException

import articles
import podcast

FEED_REFRESH_SECONDS = 3600
TTS_BYTES_PER_SECOND = 6000  # Substack's TTS is 48 kbps CBR
# A new free post waits this long for Substack's TTS, out of the feed, before Gemini reads it instead.
TTS_WAIT_SECONDS = 86400

log = logging.getLogger("articlecast")

# host -> (time started, task refreshing its channel and posts); failed refreshes are dropped
refreshes = {}
# Substack rate-limits requests from one IP across all publications, so they go one at a time.
substack_turn = asyncio.Lock()


async def substack_get(http, url):
    """Returns the status and body of a GET, waiting out Substack's rate limit."""
    async with substack_turn:
        for _ in range(6):
            async with http.get(url) as response:
                if response.status != 429:
                    return response.status, await response.read()
                wait = int(response.headers.get("Retry-After", 10))
            await asyncio.sleep(wait)
    raise RuntimeError(f"{url} is still rate limited")


def own_audio(post):
    """The URL and duration of a post's own audio: Substack's TTS, whose duration follows from its size, or its
    podcast episode, whose duration is 0 until Substack knows it."""
    tts = next((item["audio_url"] for item in post.get("audio_items") or []
                if item["type"] == "tts" and item["status"] == "completed" and item["audio_url"]), None)
    if tts:
        return tts, None
    if post["podcast_url"]:
        return post["podcast_url"], int(post["podcast_duration"] or 0)
    return None, None


def awaits_audio(post):
    return post["audio_url"] is None and time.time() - post["published"] < TTS_WAIT_SECONDS


async def refresh_posts(http, path, host):
    """Adds archive posts newer than the saved ones to the file at `path`, and returns all posts, newest first."""
    saved = {p["id"]: p for p in json.loads(path.read_text())} if path.exists() else {}
    posts = {id: dict(post) for id, post in saved.items()}
    offset = 0
    while True:
        status, body = await substack_get(http, f"https://{host}/api/v1/archive?sort=new&offset={offset}&limit=50")
        if status != 200:
            raise RuntimeError(f"{host} archive API returned {status}")
        page = json.loads(body)
        for post in page:
            audio_url, duration = own_audio(post)
            old = saved.get(post["id"])
            same_audio = old and old["audio_url"] == audio_url
            posts[post["id"]] = {
                "id": post["id"], "slug": post["slug"], "title": post["title"], "subtitle": post["subtitle"] or "",
                "url": post["canonical_url"], "published": timegm(datetime.fromisoformat(post["post_date"]).utctimetuple()),
                "audio_url": audio_url, "size": old["size"] if same_audio else None, "duration": old["duration"] if same_audio else duration,
                "free": post["audience"] == "everyone", "article": old["article"] if old else None}
        if not page or any(post["id"] in saved for post in page):
            break
        offset += len(page)
    head_slots = asyncio.Semaphore(16)

    async def measure(post):
        async with head_slots, http.head(post["audio_url"], allow_redirects=True) as response:
            if response.status != 200 or "Content-Length" not in response.headers:
                log.warning("leaving out %s: its audio returned %d without a size", post["url"], response.status)
                post["audio_url"] = None
                return
            post["size"] = int(response.headers["Content-Length"])
            if post["duration"] is None:
                post["duration"] = post["size"] // TTS_BYTES_PER_SECOND

    await asyncio.gather(*(measure(p) for p in posts.values() if p["audio_url"] and p["size"] is None))
    for post in posts.values():
        if post["free"] and post["audio_url"] is None and post["article"] is None and not awaits_audio(post):
            try:
                status, body = await substack_get(http, f"https://{host}/api/v1/posts/{post['slug']}")
            except Exception as error:
                log.error("will retry %s: %r", post["url"], error)
                continue
            if status not in (200, 404):
                log.error("will retry %s: it returned %d", post["url"], status)
                continue
            text = articles.speech_text(json.loads(body)["body_html"] or "<p></p>") if status == 200 else ""
            if not text:
                log.warning("leaving out %s: no text to read (status %d)", post["url"], status)
                post["free"] = False
                continue
            post["article"] = articles.save(post["url"], post["title"], post["published"], f"{post['title']}.\n{text}")
    if posts != saved:
        path.write_text(json.dumps(list(posts.values())))
    return sorted(posts.values(), key=lambda p: p["published"], reverse=True)


async def refresh(http, path, host):
    status, body = await substack_get(http, f"https://{host}/feed")
    channel = feedparser.parse(body).feed if status == 200 else {}
    # The feed is public, so it only serves real Substack publications.
    if channel.get("generator") != "Substack":
        raise HTTPException(404, f"{host} is not a Substack publication")
    return channel, await refresh_posts(http, path, host)


async def feed_xml(http, store, host, article_audio_base):
    started, task = refreshes.get(host, (0, None))
    if time.time() - started > FEED_REFRESH_SECONDS:
        task = asyncio.create_task(refresh(http, store / f"{host}.json", host))
        task.add_done_callback(lambda t: t.exception() and refreshes.pop(host))
        refreshes[host] = (time.time(), task)
    # A first refresh pages through the whole archive; shielding it keeps that work for the next request
    # if the client gives up.
    channel, posts = await asyncio.shield(task)
    # Built on every request, so episodes Gemini has read show their final size.
    items = [{"title": post["title"], "description": post["subtitle"], "link": post["url"], "guid": f"substack-{post['id']}", "published": post["published"],
              **({"url": post["audio_url"], "size": post["size"], "duration": post["duration"]} if post["audio_url"]
                 else articles.enclosure(post["article"], article_audio_base))}
             for post in posts if post["audio_url"] or post["article"]]
    image = channel.get("image", {}).get("href")
    if image:
        if not image.startswith("https://substackcdn.com/image/fetch/"):
            raise RuntimeError(f"{host} logo {image} is not on Substack's image CDN")
        # Podcast apps draw transparency black, which hides logos drawn as cut-outs, so the CDN
        # serves the full-size original flattened onto white.
        image = f"https://substackcdn.com/image/fetch/f_png,b_rgb:ffffff/{image.rsplit('/', 1)[1]}"
    return podcast.feed_xml(title=channel["title"], link=f"https://{host}", description=channel.get("description", ""),
                            author=channel.get("author", channel["title"]), image=image, items=items)
