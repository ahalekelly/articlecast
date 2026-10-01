"""Podcast feed of a LessWrong author's posts and Quick Takes.

Posts play the narrations TYPE III AUDIO makes for LessWrong. Quick Takes, and posts still without a
narration a day after publishing, are saved as articles for Gemini to read. Each author's episodes are saved as
JSON, so episodes stay after they drop out of the author's newest posts.
"""

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from urllib.parse import quote

from starlette.exceptions import HTTPException

import articles
import podcast

FEED_REFRESH_SECONDS = 3600
NEWEST_ITEMS = 50  # posts and Quick Takes fetched per refresh
NARRATED_SINCE = datetime(2023, 7, 1, tzinfo=UTC).timestamp()  # TYPE III narrates posts from this date
# A new post waits this long for its TYPE III narration, out of the feed, before Gemini reads it instead.
NARRATION_WAIT_SECONDS = 86400

log = logging.getLogger("articlecast")

# author slug -> (time started, task refreshing its episodes); failed refreshes are dropped
refreshes = {}
# LessWrong rate-limits its API, so requests go one at a time.
lesswrong_turn = asyncio.Lock()


async def graphql(http, query, **variables):
    async with lesswrong_turn, http.post("https://www.lesswrong.com/graphql", json={"query": query, "variables": variables}) as response:
        response.raise_for_status()
        body = await response.json()
    if any(e["message"] == "app.missing_document" for e in body.get("errors", [])):
        raise HTTPException(404, "no such LessWrong user")
    if "errors" in body:
        raise RuntimeError(f"LessWrong GraphQL error: {body['errors']}")
    return body["data"]


async def narration(http, post_url):
    """Returns the URL, size, and duration of the post's finished TYPE III narration, or None."""
    async with http.get(f"https://api.type3.audio/narration/find?url={quote(post_url, safe='')}&request_source=embed") as response:
        if response.status == 404:
            return None
        response.raise_for_status()
        found = await response.json()
    if found["status"] != "Succeeded":
        return None
    # mp3_url redirects to a signed file URL that expires within a day, so the feed links mp3_url.
    async with http.head(found["mp3_url"], allow_redirects=True) as response:
        response.raise_for_status()
        return {"url": found["mp3_url"], "size": int(response.headers["Content-Length"]), "duration": int(found["duration"])}


def awaits_narration(episode):
    return (episode["is_post"] and episode["audio"] is None and episode["published"] >= NARRATED_SINCE
            and time.time() - episode["published"] < NARRATION_WAIT_SECONDS)


async def refresh_episodes(http, path, slug):
    """Adds the author's newest posts and Quick Takes to the episodes saved at `path`, and returns
    the author and all episodes, newest first."""
    saved = {e["id"]: e for e in json.loads(path.read_text())} if path.exists() else {}
    user = (await graphql(http, "query($slug: String) { user(input: {selector: {slug: $slug}}) { result { _id displayName shortformFeedId biography { plaintextDescription } } } }",
                          slug=slug))["user"]["result"]
    fields = "_id pageUrl postedAt contents { html }"
    items = (await graphql(http, f"query($userId: String) {{ posts(input: {{terms: {{view: \"userPosts\", userId: $userId, limit: {NEWEST_ITEMS}}}}}) {{ results {{ title {fields} }} }} }}",
                           userId=user["_id"]))["posts"]["results"]
    if user["shortformFeedId"]:
        comments = (await graphql(http, f"query($postId: String, $userId: String) {{ comments(input: {{terms: {{view: \"postCommentsNew\", postId: $postId, userId: $userId, limit: {NEWEST_ITEMS}}}}}) {{ results {{ parentCommentId {fields} }} }} }}",
                                  postId=user["shortformFeedId"], userId=user["_id"]))["comments"]["results"]
        items += [c for c in comments if c["parentCommentId"] is None]
    episodes = {e["id"]: dict(e) for e in saved.values()}
    unchecked = []
    for item in items:
        if item["_id"] in episodes:
            continue
        text = articles.speech_text(item["contents"]["html"]) if item["contents"] else ""
        if not text:
            continue
        is_post = "title" in item
        words = text.split()
        title = item["title"] if is_post else "Quick Take: " + " ".join(words[:10]) + ("…" if len(words) > 10 else "")
        published = datetime.fromisoformat(item["postedAt"]).timestamp()
        article = articles.save(item["pageUrl"], title, published, f"{title}.\n{text}" if is_post else text)
        episode = episodes[item["_id"]] = {"id": item["_id"], "title": title, "url": item["pageUrl"], "published": published,
                                           "is_post": is_post, "article": article, "audio": None}
        if is_post and published >= NARRATED_SINCE:
            unchecked.append(episode)
    lookups = unchecked + [e for e in episodes.values() if awaits_narration(e) and e not in unchecked]
    for episode, audio in zip(lookups, await asyncio.gather(*(narration(http, e["url"]) for e in lookups), return_exceptions=True)):
        if isinstance(audio, Exception):
            log.error("TYPE III lookup failed for %s: %r", episode["url"], audio)
        else:
            episode["audio"] = audio
    if episodes != saved:
        path.write_text(json.dumps(list(episodes.values())))
    return user, sorted(episodes.values(), key=lambda e: e["published"], reverse=True)


async def feed_xml(http, store, slug, article_audio_base):
    started, task = refreshes.get(slug, (0, None))
    if time.time() - started > FEED_REFRESH_SECONDS:
        task = asyncio.create_task(refresh_episodes(http, store / f"{slug}.json", slug))
        task.add_done_callback(lambda t: t.exception() and refreshes.pop(slug))
        refreshes[slug] = (time.time(), task)
    user, episodes = await asyncio.shield(task)
    # Built on every request, so episodes Gemini has read show their final size.
    items = [{"title": e["title"], "description": e["url"], "link": e["url"], "guid": f"lesswrong-{e['id']}", "published": e["published"],
              **(e["audio"] or articles.enclosure(e["article"], article_audio_base))} for e in episodes if not awaits_narration(e)]
    return podcast.feed_xml(title=f"{user['displayName']} on LessWrong", link=f"https://www.lesswrong.com/users/{slug}",
                            description=(user["biography"] or {}).get("plaintextDescription") or "",
                            author=user["displayName"], image=None, items=items)
