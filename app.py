"""Podcast feed of articles from RSS feeds, read aloud by Gemini on first download.

The feed lists every recent article with an estimated audio size. Nothing is synthesized
until Pocket Casts on a phone downloads an episode. The first download streams audio as
Gemini generates it, padded with silence to the size the feed promised. Later downloads
get the finished file.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import time
from calendar import timegm
from email.utils import formatdate
from pathlib import Path
from xml.sax.saxutils import escape

import aiohttp
import feedparser
import trafilatura
from google import genai
from google.genai import types
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, Response, StreamingResponse
from starlette.routing import Route

import substack

STORE = Path(os.environ["STORE_DIR"])
TOKEN = os.environ["FEED_TOKEN"]
FEEDS = [line.strip() for line in (Path(__file__).parent / "feeds.txt").read_text().splitlines() if line.strip() and not line.startswith("#")]
gemini = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

MODEL = "gemini-3.8-flash-tts"
VOICE = "Kore"
# Only the phone app may trigger synthesis. Pocket Casts' servers also download episodes
# (as "WordPress.com - Audio"), which would synthesize every article in the feed.
SYNTHESIS_USER_AGENTS = {"Pocket Casts"}
ARTICLES_IN_FEED = 30
FEED_REFRESH_SECONDS = 600
CHUNK_WORDS = 200  # words per Gemini request
PARALLEL_REQUESTS = 3
CHUNK_GAP_SECONDS = 0.4
# Gemini Flash reads about 170 words per minute. The estimate runs long on purpose: a first
# listen that runs past the estimate is cut off, one that falls short ends in silence.
WORDS_PER_SECOND = 170 / 60
ESTIMATE_MARGIN = 1.25
# 128 kbps CBR at 24 kHz mono: every MP3 frame is 72 * 128000 / 24000 = 384 bytes.
BYTES_PER_SECOND = 16000
FRAME_BYTES = 384
PCM_BYTES_PER_SECOND = 48000  # Gemini returns 24 kHz 16-bit mono

log = logging.getLogger("articlecast")


def article_path(article_id):
    return STORE / "articles" / f"{article_id}.json"


def audio_path(article_id):
    return STORE / "audio" / f"{article_id}.mp3"


def estimated_size(words):
    seconds = words / WORDS_PER_SECOND * ESTIMATE_MARGIN + 5
    return math.ceil(seconds * BYTES_PER_SECOND / FRAME_BYTES) * FRAME_BYTES


def ffmpeg_args(*input_args):
    return ["ffmpeg", "-loglevel", "error", *input_args, "-ac", "1", "-c:a", "libmp3lame", "-b:a", "128k",
            "-write_xing", "0", "-id3v2_version", "0", "-f", "mp3", "pipe:1"]


async def encode_silent_frame():
    proc = await asyncio.create_subprocess_exec(
        *ffmpeg_args("-f", "lavfi", "-i", "anullsrc=r=24000:cl=mono", "-t", "2"), stdout=asyncio.subprocess.PIPE)
    mp3, _ = await proc.communicate()
    frame = mp3[FRAME_BYTES * 20 : FRAME_BYTES * 21]
    if len(mp3) % FRAME_BYTES or frame[:2] != b"\xff\xf3":
        raise RuntimeError(f"ffmpeg output is not {FRAME_BYTES}-byte MPEG-2 frames")
    return frame


class Library:
    """Recent articles from the configured feeds, with their extracted text."""

    def __init__(self):
        self.articles = []
        self.refreshed = 0.0
        self.lock = asyncio.Lock()

    async def recent(self, http):
        async with self.lock:
            if time.time() - self.refreshed > FEED_REFRESH_SECONDS:
                self.articles = await self.refresh(http)
                self.refreshed = time.time()
            return self.articles

    async def refresh(self, http):
        entries = []
        for feed_url in FEEDS:
            async with http.get(feed_url) as response:
                response.raise_for_status()
                parsed = feedparser.parse(await response.read())
            for entry in parsed.entries:
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                entries.append({"url": entry.link, "title": entry.get("title", entry.link), "feed": parsed.feed.get("title", feed_url),
                                "published": timegm(published) if published else time.time()})
        entries.sort(key=lambda e: e["published"], reverse=True)
        extract_slots = asyncio.Semaphore(8)

        async def load(entry):
            article_id = hashlib.sha1(entry["url"].encode()).hexdigest()[:16]
            path = article_path(article_id)
            if path.exists():
                return json.loads(path.read_text())
            async with extract_slots, http.get(entry["url"]) as response:
                response.raise_for_status()
                html = await response.text()
            text = await asyncio.to_thread(trafilatura.extract, html, favor_precision=True)
            if not text:
                raise ValueError(f"no article text found at {entry['url']}")
            speech = f"{entry['title']}.\n{text}"
            article = {**entry, "id": article_id, "text": speech, "size": estimated_size(len(speech.split()))}
            path.write_text(json.dumps(article))
            return article

        results = await asyncio.gather(*(load(e) for e in entries[:ARTICLES_IN_FEED]), return_exceptions=True)
        for entry, result in zip(entries, results):
            if isinstance(result, Exception):
                log.error("skipping %s: %r", entry["url"], result)
        return [r for r in results if not isinstance(r, Exception)]


class Synthesis:
    """One article's audio as it is generated, readable by any number of requests at once.

    Readers see exactly `size` bytes, the size the feed promised: generated audio followed
    by silent frames, or cut off if the article ran longer than estimated. `data` keeps the
    full audio for the saved file.
    """

    def __init__(self, article, silent_frame):
        self.article = article
        self.size = article["size"]
        self.silent_frame = silent_frame
        self.data = bytearray()
        self.finished = False
        self.failed = False
        self.changed = asyncio.Condition()
        self.task = asyncio.create_task(self.run())

    async def read(self, start, stop):
        position = start
        while position < stop:
            async with self.changed:
                await self.changed.wait_for(lambda: len(self.data) > position or self.finished or self.failed)
            if self.failed:
                raise RuntimeError(f"synthesis failed for {self.article['id']}")
            end = min(stop, position + 65536)
            if position < len(self.data):
                piece = bytes(self.data[position : min(end, len(self.data))])
            else:
                offset = (position - len(self.data)) % FRAME_BYTES
                piece = (self.silent_frame * ((end - position) // FRAME_BYTES + 2))[offset : offset + end - position]
            yield piece
            position += len(piece)

    async def append(self, piece):
        async with self.changed:
            self.data += piece
            self.changed.notify_all()

    async def run(self):
        started = time.time()
        try:
            encoder = await asyncio.create_subprocess_exec(
                *ffmpeg_args("-f", "s16le", "-ar", "24000", "-ac", "1", "-i", "pipe:0"),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
            collect = asyncio.create_task(self.collect(encoder.stdout))
            for i, pcm_pieces in enumerate(self.generate_chunks()):
                if i:
                    encoder.stdin.write(bytes(int(PCM_BYTES_PER_SECOND * CHUNK_GAP_SECONDS) // 2 * 2))
                async for pcm in pcm_pieces:
                    encoder.stdin.write(pcm)
                    await encoder.stdin.drain()
            encoder.stdin.close()
            await collect
            if await encoder.wait():
                raise RuntimeError("ffmpeg failed")
            audio_path(self.article["id"]).write_bytes(self.data)
            if len(self.data) > self.size:
                log.warning("%s ran %d bytes past its estimate; its first listen was cut off", self.article["id"], len(self.data) - self.size)
            async with self.changed:
                self.finished = True
                self.changed.notify_all()
            log.info("synthesized %s: %.0f s of audio in %.0f s", self.article["id"], len(self.data) / BYTES_PER_SECOND, time.time() - started)
        except Exception:
            log.exception("synthesis failed for %s", self.article["id"])
            async with self.changed:
                self.failed = True
                self.changed.notify_all()
            raise

    async def collect(self, stdout):
        while piece := await stdout.read(FRAME_BYTES * 16):
            await self.append(piece)

    def generate_chunks(self):
        """Yields, in order, an async iterator of PCM for each chunk; chunks generate in parallel."""
        chunks, current = [], []
        for paragraph in self.article["text"].split("\n"):
            current.append(paragraph)
            if sum(len(p.split()) for p in current) >= CHUNK_WORDS:
                chunks.append("\n".join(current))
                current = []
        if current:
            chunks.append("\n".join(current))
        slots = asyncio.Semaphore(PARALLEL_REQUESTS)
        queues = [asyncio.Queue() for _ in chunks]

        async def produce(text, queue):
            try:
                await stream_chunk(text, queue)
            finally:
                await queue.put(None)

        async def stream_chunk(text, queue):
            async with slots:
                stream = await gemini.aio.models.generate_content_stream(model=MODEL, contents=text, config=types.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
                    speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=VOICE)))))
                async for response in stream:
                    for part in response.candidates[0].content.parts or []:
                        if part.inline_data:
                            await queue.put(part.inline_data.data)

        producers = [asyncio.create_task(produce(text, queue)) for text, queue in zip(chunks, queues)]

        async def drain(queue, producer):
            while (pcm := await queue.get()) is not None:
                yield pcm
            await producer  # re-raises a failed request

        return [drain(queue, producer) for queue, producer in zip(queues, producers)]


def feed_xml(base, articles):
    items = []
    for a in articles:
        path = audio_path(a["id"])
        size = path.stat().st_size if path.exists() else a["size"]
        items.append(f"""
    <item>
      <title>{escape(a["title"])}</title>
      <description>{escape(a["feed"])}: {escape(a["url"])}</description>
      <link>{escape(a["url"])}</link>
      <guid isPermaLink="false">{a["id"]}</guid>
      <pubDate>{formatdate(a["published"], usegmt=True)}</pubDate>
      <enclosure url="{base}/{TOKEN}/audio/{a["id"]}.mp3" length="{size}" type="audio/mpeg"/>
      <itunes:duration>{size // BYTES_PER_SECOND}</itunes:duration>
    </item>""")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Articlecast</title>
    <link>{base}</link>
    <description>Articles from my feeds, read aloud.</description>
    <language>en-us</language>
    <itunes:author>Articlecast</itunes:author>
    <itunes:explicit>false</itunes:explicit>{"".join(items)}
  </channel>
</rss>
"""


def check_token(request):
    if request.path_params["token"] != TOKEN:
        raise HTTPException(404)


async def feed(request):
    check_token(request)
    state = request.app.state
    articles = await state.library.recent(state.http)
    return Response(feed_xml(f"https://{request.url.netloc}", articles), media_type="application/rss+xml")


async def substack_feed(request):
    xml = await substack.feed_xml(request.app.state.http, request.path_params["host"])
    return Response(xml, media_type="application/rss+xml")


def byte_range(header, size):
    """Parses a single-range `bytes=` header into a half-open [start, stop) range."""
    if not header:
        return 0, size
    first, _, last = header.removeprefix("bytes=").split(",")[0].strip().partition("-")
    if not first:
        return max(0, size - int(last)), size
    return int(first), min(int(last) + 1, size) if last else size


async def audio(request):
    check_token(request)
    article_id = request.path_params["id"]
    user_agent = request.headers.get("user-agent", "")
    range_header = request.headers.get("range")
    log.info("%s %s range=%s ua=%r", request.method, article_id, range_header, user_agent)
    path = audio_path(article_id)
    if path.exists():
        return FileResponse(path, media_type="audio/mpeg")
    if not article_path(article_id).exists():
        raise HTTPException(404)
    article = json.loads(article_path(article_id).read_text())
    syntheses = request.app.state.syntheses
    synthesis = syntheses.get(article_id)
    if request.method == "GET" and (synthesis is None or synthesis.failed):
        if user_agent not in SYNTHESIS_USER_AGENTS:
            log.warning("refused synthesis of %s for %r", article_id, user_agent)
            raise HTTPException(403)
        synthesis = syntheses[article_id] = Synthesis(article, request.app.state.silent_frame)
    size = article["size"]
    start, stop = byte_range(range_header, size)
    if not 0 <= start < stop:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
    headers = {"Content-Length": str(stop - start), "Accept-Ranges": "bytes"}
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{stop - 1}/{size}"
    status = 206 if range_header else 200
    if request.method == "HEAD":
        return Response(status_code=status, headers=headers, media_type="audio/mpeg")
    return StreamingResponse(synthesis.read(start, stop), status_code=status, headers=headers, media_type="audio/mpeg")


@contextlib.asynccontextmanager
async def lifespan(app):
    (STORE / "articles").mkdir(parents=True, exist_ok=True)
    (STORE / "audio").mkdir(parents=True, exist_ok=True)
    app.state.silent_frame = await encode_silent_frame()
    app.state.library = Library()
    app.state.syntheses = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), headers={"User-Agent": "Mozilla/5.0 (compatible; Articlecast)"}) as http:
        app.state.http = http
        yield


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
app = Starlette(lifespan=lifespan, routes=[
    Route("/{token}/feed.xml", feed),
    Route("/{token}/audio/{id}.mp3", audio, methods=["GET", "HEAD"]),
    Route("/substack/{host}/feed.xml", substack_feed),
])
