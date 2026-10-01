"""The server's text-to-speech model and voice, how text is split into requests, and each provider's
streaming call. Every provider returns 24 kHz 16-bit mono PCM.

The model and voice match the read-aloud in Adrian's T3 Code fork (apps/mobile/src/lib/speechSettings.ts).
"""

import logging
import os
import re
import time
from datetime import UTC, datetime

import aiohttp
from xml.sax.saxutils import escape

from google import genai
from google.genai import types

GEMINI_VOICES = (
    "Kore", "Zephyr", "Puck", "Charon", "Fenrir", "Leda", "Orus", "Aoede", "Callirrhoe", "Autonoe", "Enceladus",
    "Iapetus", "Umbriel", "Algieba", "Despina", "Erinome", "Algenib", "Rasalgethi", "Laomedeia", "Achernar",
    "Alnilam", "Schedar", "Gacrux", "Pulcherrima", "Achird", "Zubenelgenubi", "Vindemiatrix", "Sadachbia",
    "Sadaltager", "Sulafat",
)
MAI_VOICES = ("Harper", "Olivia", "Iris", "Ethan", "Grant", "Jasper", "Sage")  # US English
MODELS = {
    "gemini-3.8-flash-tts": GEMINI_VOICES,
    "gemini-3.8-flash-lite-tts": GEMINI_VOICES,
    "MAI-Voice-2.1": MAI_VOICES,
    "MAI-Voice-2.1-Flash": MAI_VOICES,
}
MAX_CHUNK_CHARS = 2000  # characters per request
# Azure Speech region, near the Cloud Run region; MAI voices run only in some regions, and a key works only in its own.
AZURE_REGION = "westus2"

MODEL = os.environ["TTS_MODEL"]
VOICE = os.environ["TTS_VOICE"]
if MODEL not in MODELS:
    raise ValueError(f"TTS_MODEL {MODEL!r} is not one of {', '.join(MODELS)}")
if VOICE not in MODELS[MODEL]:
    raise ValueError(f"TTS_VOICE {VOICE!r} is not a {MODEL} voice: {', '.join(MODELS[MODEL])}")
# Names this model and voice's saved audio, so changing either reads articles afresh.
READER = f"{MODEL}/{VOICE}"
if MODEL.startswith("MAI-"):
    # MAI requests go to a free-tier (F0) resource first, and to a standard (S0) one when F0 refuses.
    AZURE_SPEECH_FREE_KEY = os.environ["AZURE_SPEECH_FREE_KEY"]
    AZURE_SPEECH_KEY = os.environ["AZURE_SPEECH_KEY"]
else:
    gemini = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

log = logging.getLogger("articlecast")
# When F0's monthly quota runs out, requests skip it until this time.
free_quota_resets = 0.0


def next_second_of_month():
    """F0's quota refills at the start of the resource's billing cycle, usually the 1st, at a time Azure doesn't
    document. The next 2nd at 00:00 UTC is past the 1st in every time zone, so an early retry can't skip a month."""
    now = datetime.now(UTC)
    second = datetime(now.year, now.month, 2, tzinfo=UTC)
    return (second if second > now else datetime(now.year + now.month // 12, now.month % 12 + 1, 2, tzinfo=UTC)).timestamp()

SENTENCE_END = re.compile(r"(?:(?<=[.!?…])|(?<=[.!?…][\"'”’)\]]))\s+")


def chunks(text):
    """Packs whole sentences into requests of up to MAX_CHUNK_CHARS. A paragraph break inside a request
    stays a line break; a sentence longer than a request is split between words."""
    packed, pending = [], ""

    def add(part, separator):
        nonlocal pending
        if pending and len(pending) + 1 + len(part) > MAX_CHUNK_CHARS:
            packed.append(pending)
            pending = ""
        pending = f"{pending}{separator}{part}" if pending else part

    for paragraph in text.split("\n"):
        separator = "\n"
        for sentence in SENTENCE_END.split(paragraph.strip()):
            if not sentence:
                continue
            pieces = [sentence] if len(sentence) <= MAX_CHUNK_CHARS else [
                word[i : i + MAX_CHUNK_CHARS] for word in sentence.split() for i in range(0, len(word), MAX_CHUNK_CHARS)]
            for piece in pieces:
                add(piece, separator)
                separator = " "
    if pending:
        packed.append(pending)
    return packed


async def stream(http, text):
    """Yields the PCM of `text` read aloud as it arrives."""
    received = 0
    async for pcm in provider_stream(http, text):
        received += len(pcm)
        yield pcm
    if not received:
        raise RuntimeError(f"{MODEL} returned no audio")


async def provider_stream(http, text):
    global free_quota_resets
    if MODEL.startswith("MAI-"):
        # MAI reads square brackets as delivery tags: an unknown one is dropped, starts a sentence with a 400, or
        # stalls a long request. Curly braces also lose words. Parentheses read as written.
        text = text.translate(str.maketrans("[]{}", "()()"))
        ssml = f'<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-US"><voice name="en-US-{VOICE}:{MODEL}">{escape(text)}</voice></speak>'
        keys = [AZURE_SPEECH_KEY] if time.time() < free_quota_resets else [AZURE_SPEECH_FREE_KEY, AZURE_SPEECH_KEY]
        for key in keys:
            async with http.post(f"https://{AZURE_REGION}.tts.speech.microsoft.com/cognitiveservices/v1", data=ssml.encode(), headers={
                "Ocp-Apim-Subscription-Key": key, "Content-Type": "application/ssml+xml",
                "X-Microsoft-OutputFormat": "raw-24khz-16bit-mono-pcm"},
                # A long request streams for minutes, so only a stalled connection times out.
                timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60)) as response:
                # F0 answers 429 while over its rate limit and 403 once its monthly quota is spent.
                if key == AZURE_SPEECH_FREE_KEY and response.status in (403, 429):
                    if response.status == 403:
                        free_quota_resets = time.time() + int(response.headers["Retry-After"]) if "Retry-After" in response.headers else next_second_of_month()
                    log.warning("Azure free tier refused (%d: %s); using the standard tier", response.status, await response.text())
                    continue
                if response.status != 200:
                    raise RuntimeError(f"Azure speech failed ({response.status}): {await response.text()}")
                async for pcm in response.content.iter_chunked(65536):
                    yield pcm
                return
    responses = await gemini.aio.models.generate_content_stream(model=MODEL, contents=text, config=types.GenerateContentConfig(
        response_modalities=["AUDIO"],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=VOICE)))))
    async for response in responses:
        for part in response.candidates[0].content.parts or []:
            if part.inline_data:
                yield part.inline_data.data
