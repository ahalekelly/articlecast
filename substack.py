"""Podcast feed of a Substack publication, using the text-to-speech audio Substack generates for its app.

Substack's undocumented archive API lists every post with its TTS MP3 on S3. The feed links those
files directly, so nothing is synthesized here. Each publication's posts are saved as JSON, and a
refresh fetches only archive pages newer than the saved posts.
"""

import asyncio
import json
import logging
import time
from calendar import timegm
from datetime import datetime

import feedparser
from starlette.exceptions import HTTPException

import podcast

FEED_REFRESH_SECONDS = 3600
TTS_BYTES_PER_SECOND = 6000  # Substack's TTS is 48 kbps CBR

log = logging.getLogger("articlecast")

# host -> (time started, task building its feed XML); failed builds are dropped
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


async def refresh_posts(http, path, host):
    """Adds archive posts newer than the saved ones to the file at `path`, and returns all posts, newest first."""
    saved = {p["id"]: p for p in json.loads(path.read_text())} if path.exists() else {}
    posts = dict(saved)
    offset = 0
    while True:
        status, body = await substack_get(http, f"https://{host}/api/v1/archive?sort=new&offset={offset}&limit=50")
        if status != 200:
            raise RuntimeError(f"{host} archive API returned {status}")
        page = json.loads(body)
        for post in page:
            audio_url = next((item["audio_url"] for item in post.get("audio_items") or []
                              if item["type"] == "tts" and item["status"] == "completed" and item["audio_url"]), None)
            old = saved.get(post["id"])
            posts[post["id"]] = {
                "id": post["id"], "title": post["title"], "subtitle": post["subtitle"] or "",
                "url": post["canonical_url"], "published": timegm(datetime.fromisoformat(post["post_date"]).utctimetuple()),
                "audio_url": audio_url, "size": old["size"] if old and old["audio_url"] == audio_url else None}
        if not page or any(post["id"] in saved for post in page):
            break
        offset += len(page)
    head_slots = asyncio.Semaphore(16)

    async def measure(post):
        async with head_slots, http.head(post["audio_url"]) as response:
            if response.status != 200:
                log.warning("leaving out %s: its audio returned %d", post["url"], response.status)
                post["audio_url"] = None
                return
            post["size"] = int(response.headers["Content-Length"])

    await asyncio.gather(*(measure(p) for p in posts.values() if p["audio_url"] and p["size"] is None))
    if posts != saved:
        path.write_text(json.dumps(list(posts.values())))
    return sorted(posts.values(), key=lambda p: p["published"], reverse=True)


async def build_feed(http, path, host):
    status, body = await substack_get(http, f"https://{host}/feed")
    channel = feedparser.parse(body).feed if status == 200 else {}
    # The feed is public, so it only serves real Substack publications.
    if channel.get("generator") != "Substack":
        raise HTTPException(404, f"{host} is not a Substack publication")
    items = [{"title": post["title"], "description": post["subtitle"], "link": post["url"], "guid": f"substack-{post['id']}",
              "published": post["published"], "url": post["audio_url"], "size": post["size"], "duration": post["size"] // TTS_BYTES_PER_SECOND}
             for post in await refresh_posts(http, path, host) if post["audio_url"]]
    image = channel.get("image", {}).get("href")
    if image:
        if not image.startswith("https://substackcdn.com/image/fetch/"):
            raise RuntimeError(f"{host} logo {image} is not on Substack's image CDN")
        # Podcast apps draw transparency black, which hides logos drawn as cut-outs, so the CDN
        # serves the full-size original flattened onto white.
        image = f"https://substackcdn.com/image/fetch/f_png,b_rgb:ffffff/{image.rsplit('/', 1)[1]}"
    return podcast.feed_xml(title=channel["title"], link=f"https://{host}", description=channel.get("description", ""),
                            author=channel.get("author", channel["title"]), image=image, items=items)


async def feed_xml(http, store, host):
    started, task = refreshes.get(host, (0, None))
    if time.time() - started > FEED_REFRESH_SECONDS:
        task = asyncio.create_task(build_feed(http, store / f"{host}.json", host))
        task.add_done_callback(lambda t: t.exception() and refreshes.pop(host))
        refreshes[host] = (time.time(), task)
    # A first build pages through the whole archive; shielding it keeps that work for the next request
    # if the client gives up.
    return await asyncio.shield(task)
