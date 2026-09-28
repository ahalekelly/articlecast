"""Podcast feed of a Substack publication, using the text-to-speech audio Substack generates for its app.

Substack's undocumented posts API lists each post's TTS MP3 on S3. The feed links those files
directly, so nothing is synthesized or stored here.
"""

import asyncio
import time
from calendar import timegm
from datetime import datetime
from email.utils import formatdate
from xml.sax.saxutils import escape

import feedparser

POSTS_IN_FEED = 30
FEED_REFRESH_SECONDS = 600
TTS_BYTES_PER_SECOND = 6000  # Substack's TTS is 48 kbps CBR

# host -> (time built, feed XML)
feeds = {}
# S3 audio URL -> size in bytes; each URL holds one immutable file
sizes = {}


async def audio_size(http, url):
    if url not in sizes:
        async with http.head(url) as response:
            response.raise_for_status()
            sizes[url] = int(response.headers["Content-Length"])
    return sizes[url]


async def build_feed(http, host):
    async with http.get(f"https://{host}/feed") as response:
        response.raise_for_status()
        channel = feedparser.parse(await response.read()).feed
    async with http.get(f"https://{host}/api/v1/posts", params={"limit": POSTS_IN_FEED}) as response:
        response.raise_for_status()
        posts = await response.json()
    episodes = [(post, item["audio_url"]) for post in posts for item in post["audio_items"] or []
                if item["type"] == "tts" and item["status"] == "completed" and item["audio_url"]]
    episode_sizes = await asyncio.gather(*(audio_size(http, url) for _, url in episodes))
    items = []
    for (post, url), size in zip(episodes, episode_sizes):
        published = timegm(datetime.fromisoformat(post["post_date"]).utctimetuple())
        items.append(f"""
    <item>
      <title>{escape(post["title"])}</title>
      <description>{escape(post["subtitle"] or "")}</description>
      <link>{escape(post["canonical_url"])}</link>
      <guid isPermaLink="false">substack-{post["id"]}</guid>
      <pubDate>{formatdate(published, usegmt=True)}</pubDate>
      <enclosure url="{escape(url)}" length="{size}" type="audio/mpeg"/>
      <itunes:duration>{size // TTS_BYTES_PER_SECOND}</itunes:duration>
    </item>""")
    image = channel.get("image", {}).get("href")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>{escape(channel["title"])} (read aloud)</title>
    <link>https://{host}</link>
    <description>{escape(channel.get("description", ""))}</description>
    <language>en-us</language>
    <itunes:author>{escape(channel.get("author", channel["title"]))}</itunes:author>{f'''
    <itunes:image href="{escape(image)}"/>''' if image else ""}
    <itunes:explicit>false</itunes:explicit>{"".join(items)}
  </channel>
</rss>
"""


async def feed_xml(http, host):
    if host not in feeds or time.time() - feeds[host][0] > FEED_REFRESH_SECONDS:
        feeds[host] = (time.time(), await build_feed(http, host))
    return feeds[host][1]
