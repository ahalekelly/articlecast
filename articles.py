"""Articles to read aloud, saved under STORE_DIR: each one's text, or for Substack posts the post id to fetch it
from on first play, and its estimated audio size, and its audio from the server's model and voice, in finished
chunks while it is read and whole once done."""

import hashlib
import json
import math
import os
import time
from pathlib import Path

import lxml.html

import tts

STORE = Path(os.environ["STORE_DIR"])
# Gemini Flash reads about 170 words per minute. The estimate runs long on purpose: a first
# listen that runs past the estimate is cut off, one that falls short ends in silence.
WORDS_PER_SECOND = 170 / 60
ESTIMATE_MARGIN = 1.25
# 128 kbps CBR at 24 kHz mono: every MP3 frame is 72 * 128000 / 24000 = 384 bytes.
BYTES_PER_SECOND = 16000
# Public feeds play this recording instead of reading an article aloud.
NO_AUDIO_SIZE = Path("no-audio.mp3").stat().st_size
FRAME_BYTES = 384
BLOCKS = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "pre")


def article_id(url):
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def article_path(article_id):
    return STORE / "articles" / f"{article_id}.json"


def audio_path(article_id):
    return STORE / "audio" / tts.READER / f"{article_id}.mp3"


def chunk_dir(article_id):
    """Holds the PCM of each chunk read so far, so a restarted reading continues at the next chunk."""
    return STORE / "chunks" / tts.READER / article_id


def estimated_size(words):
    seconds = words / WORDS_PER_SECOND * ESTIMATE_MARGIN + 5
    return math.ceil(seconds * BYTES_PER_SECOND / FRAME_BYTES) * FRAME_BYTES


def listing(article_id):
    """The saved article without its text."""
    article = json.loads(article_path(article_id).read_text())
    return {key: article[key] for key in ("id", "url", "title", "published", "size")}


def save(url, title, published, words, content):
    """Saves an article to be read aloud, with `content` holding its `text` or the id of the
    `substack_post` to fetch it from, and returns its listing."""
    article = {"id": article_id(url), "url": url, "title": title, "published": published, "size": estimated_size(words)}
    article_path(article["id"]).write_text(json.dumps({**article, **content}))
    return article


# Finished audio sizes by article id, and when they were listed. Feeds list thousands of articles, and
# checking each one's file on the storage mount takes minutes, so the folder is listed at most every 10 s.
finished = (0.0, {})


def enclosure(listing, audio_base):
    """The enclosure of a listed article, sized to its audio once synthesized."""
    global finished
    if time.time() - finished[0] > 10:
        folder = STORE / "audio" / tts.READER
        finished = (time.time(), {entry.name.removesuffix(".mp3"): entry.stat().st_size for entry in os.scandir(folder)})
    size = finished[1].get(listing["id"], listing["size"])
    return {"url": f"{audio_base}/{listing['id']}.mp3", "size": size, "duration": size // BYTES_PER_SECOND}


def read_aloud_episode(title, listing, audio_base, private):
    """The title and enclosure of an article read aloud, or in a public feed, of the notice played instead."""
    if private:
        return {"title": title, **enclosure(listing, audio_base)}
    return {"title": f"No audio: {title}", "url": f"{audio_base}/{listing['id']}.mp3",
            "size": NO_AUDIO_SIZE, "duration": NO_AUDIO_SIZE // BYTES_PER_SECOND}


def speech_text(html):
    """Paragraphs of an HTML fragment, one per line."""
    root = lxml.html.fragment_fromstring(html, create_parent="div")
    for br in root.iter("br"):
        br.tail = " " + (br.tail or "")
    blocks = [el for el in root.iter(*BLOCKS) if not any(True for _ in el.iterdescendants(*BLOCKS))]
    return "\n".join(text for el in blocks if (text := " ".join(el.text_content().split())))
