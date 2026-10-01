"""Articles to read aloud, saved under STORE_DIR: each one's text and estimated audio size, and its
audio from the server's model and voice, in finished chunks while it is read and whole once done."""

import hashlib
import json
import math
import os
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


def speech_text(html):
    """Paragraphs of an HTML fragment, one per line."""
    root = lxml.html.fragment_fromstring(html, create_parent="div")
    for br in root.iter("br"):
        br.tail = " " + (br.tail or "")
    blocks = [el for el in root.iter(*BLOCKS) if not any(True for _ in el.iterdescendants(*BLOCKS))]
    return "\n".join(text for el in blocks if (text := " ".join(el.text_content().split())))
