"""Podcast feed of a Substack publication or author, using the text-to-speech audio Substack generates for its app.

Substack's undocumented archive API lists every post of a publication, and its profile API every post an
author wrote in any publication, each with its TTS MP3 on S3, or for podcast posts the episode's MP3. The
feed links those files directly. Requests are signed in as the owner's Substack account, so posts of
publications they pay for come with their audio and text. Readable posts still without audio an hour after
publishing are saved as articles to read aloud, sized from their word count; their text is fetched on first
play, which keeps a first refresh to the archive pages. Paid posts the owner can't read often open with free
text before the paywall; for the newest few, that opening is fetched during refresh and read as a preview. Each feed's posts are saved as JSON, and a refresh
fetches only pages newer than the saved posts.
"""

import asyncio
import json
import logging
import os
import time
from calendar import timegm
from datetime import datetime
from http.cookies import SimpleCookie
from urllib.parse import quote, urlsplit

import feedparser
from yarl import URL
from starlette.exceptions import HTTPException

import articles
import podcast

FEED_REFRESH_SECONDS = 3600
TTS_BYTES_PER_SECOND = 6000  # Substack's TTS is 48 kbps CBR
# A new free post waits this long for Substack's TTS, out of the feed, before Gemini reads it instead.
TTS_WAIT_SECONDS = 3600
# The free openings of each feed's newest paid posts the owner can't read are read aloud when this long.
PREVIEWS = 10
PREVIEW_MIN_WORDS = 300

# The `substack.sid` cookie of the owner's signed-in Substack session, URL-decoded.
SUBSTACK_SID = os.environ["SUBSTACK_SID"]

log = logging.getLogger("articlecast")

# publication host or @author handle -> (time started, task refreshing its channel and posts); failed refreshes are dropped
refreshes = {}
# Substack rate-limits requests from one IP across all publications, so they go one at a time.
substack_turn = asyncio.Lock()
# Task signing in to Substack, which returns the ids of publications the owner pays for
sign_in_task = None


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


async def sign_in(http):
    """Gives the session the owner's Substack cookie, which covers *.substack.com, and a session on the custom
    domain of each publication they pay for, which Substack's sign-in redirect hands out to signed-in readers."""
    cookie = SimpleCookie()
    cookie["substack.sid"] = quote(SUBSTACK_SID, safe="")
    cookie["substack.sid"]["domain"] = "substack.com"
    http.cookie_jar.update_cookies(cookie, URL("https://substack.com/"))
    status, body = await substack_get(http, "https://substack.com/api/v1/user/profile/self")
    if status != 200:
        raise RuntimeError(f"Substack profile returned {status}: SUBSTACK_SID is no longer signed in")
    paid = [s["publication"] for s in json.loads(body)["subscriptions"] if s["membership_state"] == "subscribed"]
    for publication in paid:
        status, _ = await substack_get(http, f"https://substack.com/sign-in?redirect=%2F&for_pub={publication['subdomain']}")
        if status != 200:
            raise RuntimeError(f"signing in to {publication['subdomain']} returned {status}")
    log.info("signed in to Substack; paid publications: %s", ", ".join(p["subdomain"] for p in paid))
    return {p["id"] for p in paid}


async def paid_publications(http):
    """Ids of the publications the owner pays for, signing in on first use and again after a failed sign-in."""
    global sign_in_task
    if sign_in_task is None or sign_in_task.done() and sign_in_task.exception():
        sign_in_task = asyncio.create_task(sign_in(http))
    return await asyncio.shield(sign_in_task)


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


def description(post):
    """The post's byline, which matters for publications with several authors, then its subtitle."""
    names = [byline["name"] for byline in post["publishedBylines"]]
    byline = f"By {', '.join(names[:-1])} and {names[-1]}." if len(names) > 1 else f"By {names[0]}." if names else ""
    return f"{byline} {post['subtitle'] or ''}".strip()


def readable(post, paid):
    """Whether the owner can read a post: it is free, or its publication is one of the `paid` ids."""
    return post["free"] or post["publication_id"] in paid


def awaits_audio(post):
    return post["audio_url"] is None and time.time() - post["published"] < TTS_WAIT_SECONDS


async def archive_pages(http, host):
    """Pages of a publication's posts, newest first."""
    offset = 0
    while True:
        status, body = await substack_get(http, f"https://{host}/api/v1/archive?sort=new&offset={offset}&limit=50")
        if status != 200:
            raise RuntimeError(f"{host} archive API returned {status}")
        page = json.loads(body)
        if not page:
            return
        yield page
        offset += len(page)


async def author_pages(http, user_id):
    """Pages of the posts a user wrote in any publication, or published in one they own, newest first."""
    cursor = ""
    while True:
        status, body = await substack_get(http, f"https://substack.com/api/v1/profile/posts?profile_user_id={user_id}&limit=50&next_cursor={quote(cursor)}")
        if status != 200:
            raise RuntimeError(f"profile API returned {status} for user {user_id}")
        page = json.loads(body)
        # Besides the user's posts, the profile lists other people's posts they restacked.
        yield [post for post in page["posts"] if post["type"] != "restack"]
        cursor = page.get("nextCursor")
        if not cursor:
            return


async def refresh_posts(http, path, pages):
    """Adds posts from `pages` newer than the saved ones to the file at `path`, and returns all posts, newest first."""
    paid = await paid_publications(http)
    saved = {p["id"]: p for p in json.loads(path.read_text())} if path.exists() else {}
    posts = {id: dict(post) for id, post in saved.items()}
    async for page in pages:
        for post in page:
            audio_url, duration = own_audio(post)
            old = saved.get(post["id"])
            same_audio = old and old["audio_url"] == audio_url
            posts[post["id"]] = {
                "id": post["id"], "slug": post["slug"], "title": post["title"], "description": description(post),
                "url": post["canonical_url"], "published": timegm(datetime.fromisoformat(post["post_date"]).utctimetuple()),
                "audio_url": audio_url, "size": old["size"] if same_audio else None, "duration": old["duration"] if same_audio else duration,
                "free": post["audience"] == "everyone", "article": old["article"] if old else None,
                "publication_id": post["publication_id"], "words": post["wordcount"], "opening_words": old["opening_words"] if old else None}
        if any(post["id"] in saved for post in page):
            break
    head_slots = asyncio.Semaphore(16)

    async def measure(post):
        async with head_slots, http.head(post["audio_url"], allow_redirects=True) as response:
            # A rate-limited or failing server keeps the audio for the next refresh to measure.
            if response.status == 429 or response.status >= 500:
                log.warning("will measure %s later: its audio returned %d", post["url"], response.status)
                return
            if response.status != 200 or "Content-Length" not in response.headers:
                log.warning("leaving out %s: its audio returned %d without a size", post["url"], response.status)
                post["audio_url"] = None
                return
            post["size"] = int(response.headers["Content-Length"])
            if post["duration"] is None:
                post["duration"] = post["size"] // TTS_BYTES_PER_SECOND

    await asyncio.gather(*(measure(p) for p in posts.values() if p["audio_url"] and p["size"] is None))
    for post in posts.values():
        # A preview gives way to the whole post once its publication is paid for.
        if readable(post, paid) and post["words"] and post["audio_url"] is None and (post["article"] is None or post["opening_words"] is not None) and not awaits_audio(post):
            post["opening_words"] = None
            post["article"] = articles.save(post["url"], post["title"], post["published"], post["words"] + len(post["title"].split()),
                                            {"substack_post": f"https://{urlsplit(post['url']).netloc}/api/v1/posts/{post['slug']}"})
    unreadable = sorted((p for p in posts.values() if not readable(p, paid) and p["audio_url"] is None), key=lambda p: p["published"], reverse=True)
    for post in unreadable[:PREVIEWS]:
        if post["opening_words"] is None:
            status, body = await substack_get(http, f"https://{urlsplit(post['url']).netloc}/api/v1/posts/{post['slug']}")
            if status != 200:
                log.error("will retry the opening of %s: it returned %d", post["url"], status)
                continue
            text = articles.speech_text(json.loads(body)["body_html"] or "<p></p>")
            post["opening_words"] = len(text.split())
            if post["opening_words"] >= PREVIEW_MIN_WORDS:
                speech = f"{post['title']}.\n{text}\nThe rest of this post is for paid subscribers."
                post["article"] = articles.save(post["url"], post["title"], post["published"], len(speech.split()), {"text": speech})
    if posts != saved:
        path.write_text(json.dumps(list(posts.values())))
    return sorted(posts.values(), key=lambda p: p["published"], reverse=True)


async def post_text(http, article):
    """The text of an article saved from a Substack post, to read aloud."""
    await paid_publications(http)  # signs in, for paid posts
    status, body = await substack_get(http, article["substack_post"])
    if status != 200:
        raise HTTPException(502, f"{article['url']} returned {status}")
    text = articles.speech_text(json.loads(body)["body_html"] or "<p></p>")
    if not text:
        raise HTTPException(404, f"{article['url']} has no text to read")
    return f"{article['title']}.\n{text}"


def artwork(image):
    """Podcast apps draw transparency black, which hides logos drawn as cut-outs, so Substack's image CDN
    serves the full-size original flattened onto white."""
    original = image.rsplit("/", 1)[1] if image.startswith("https://substackcdn.com/image/fetch/") else quote(image, safe="")
    return f"https://substackcdn.com/image/fetch/f_png,b_rgb:ffffff/{original}"


async def refresh_publication(http, path, host):
    status, body = await substack_get(http, f"https://{host}/feed")
    channel = feedparser.parse(body).feed if status == 200 else {}
    # The feed is public, so it only serves real Substack publications.
    if channel.get("generator") != "Substack":
        raise HTTPException(404, f"{host} is not a Substack publication")
    image = channel.get("image", {}).get("href")
    return ({"title": channel["title"], "link": f"https://{host}", "description": channel.get("description", ""),
             "author": channel.get("author", channel["title"]), "image": image and artwork(image)},
            await refresh_posts(http, path, archive_pages(http, host)))


async def refresh_author(http, path, handle):
    status, body = await substack_get(http, f"https://substack.com/api/v1/user/{quote(handle)}/public_profile")
    if status == 404:
        raise HTTPException(404, f"{handle} is not a Substack user")
    if status != 200:
        raise RuntimeError(f"profile API returned {status} for {handle}")
    user = json.loads(body)
    return ({"title": user["name"], "link": f"https://substack.com/@{handle}", "description": user["bio"] or "",
             "author": user["name"], "image": user["photo_url"] and artwork(user["photo_url"])},
            await refresh_posts(http, path, author_pages(http, user["id"])))


async def feed_xml(http, store, source, article_audio_base, private):
    """The feed of `source`, a publication host or an @ and an author's handle. Unless `private`, it leaves out
    paid posts, and posts without Substack audio play a notice instead of being read aloud."""
    started, task = refreshes.get(source, (0, None))
    if time.time() - started > FEED_REFRESH_SECONDS:
        path = store / f"{source}.json"
        task = asyncio.create_task(refresh_author(http, path, source[1:]) if source.startswith("@") else refresh_publication(http, path, source))
        task.add_done_callback(lambda t: t.exception() and refreshes.pop(source))
        refreshes[source] = (time.time(), task)
    # A first refresh pages through the whole archive; shielding it keeps that work for the next request
    # if the client gives up.
    channel, posts = await asyncio.shield(task)
    paid = await paid_publications(http)
    # Built on every request, so episodes Gemini has read show their final size.
    items = [{"description": post["description"], "link": post["url"], "guid": f"substack-{post['id']}", "published": post["published"],
              **({"title": post["title"], "url": post["audio_url"], "size": post["size"], "duration": post["duration"]} if post["audio_url"]
                 else articles.read_aloud_episode(post["title"] if readable(post, paid) else f"Preview: {post['title']}", post["article"], article_audio_base, private))}
             for post in posts if (post["size"] if post["audio_url"] else post["article"]) and (private or post["free"])]
    return podcast.feed_xml(**channel, items=items)
