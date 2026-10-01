"""Articles for Gemini to read, saved under STORE_DIR: each one's text and estimated audio size,
and its audio once synthesized."""

import hashlib
import json
import math
import os
from pathlib import Path

STORE = Path(os.environ["STORE_DIR"])
# Gemini Flash reads about 170 words per minute. The estimate runs long on purpose: a first
# listen that runs past the estimate is cut off, one that falls short ends in silence.
WORDS_PER_SECOND = 170 / 60
ESTIMATE_MARGIN = 1.25
# 128 kbps CBR at 24 kHz mono: every MP3 frame is 72 * 128000 / 24000 = 384 bytes.
BYTES_PER_SECOND = 16000
FRAME_BYTES = 384


def article_id(url):
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def article_path(article_id):
    return STORE / "articles" / f"{article_id}.json"


def audio_path(article_id):
    return STORE / "audio" / f"{article_id}.mp3"


def estimated_size(words):
    seconds = words / WORDS_PER_SECOND * ESTIMATE_MARGIN + 5
    return math.ceil(seconds * BYTES_PER_SECOND / FRAME_BYTES) * FRAME_BYTES


def listing(article_id):
    """The saved article without its text."""
    article = json.loads(article_path(article_id).read_text())
    del article["text"]
    return article


def save(url, title, published, speech):
    """Saves an article's text to be read aloud, and returns its listing."""
    article = {"id": article_id(url), "url": url, "title": title, "published": published, "size": estimated_size(len(speech.split()))}
    article_path(article["id"]).write_text(json.dumps({**article, "text": speech}))
    return article


def enclosure(listing, audio_base):
    """The enclosure of a listed article, sized to its audio once synthesized."""
    path = audio_path(listing["id"])
    size = path.stat().st_size if path.exists() else listing["size"]
    return {"url": f"{audio_base}/{listing['id']}.mp3", "size": size, "duration": size // BYTES_PER_SECOND}
