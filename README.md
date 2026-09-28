# Articlecast

A private podcast feed of articles from RSS feeds, read aloud by Gemini 3.8 Flash TTS. Audio is generated only when you play or download an episode in Pocket Casts, so unplayed articles cost nothing.

## How it works

- The feed lists the newest articles from `feeds.txt`, with text extracted by trafilatura and an estimated audio size.
- The first download from the Pocket Casts app starts synthesis. Audio streams to the phone as Gemini generates it, several paragraphs at a time, and is padded with silence to the estimated size.
- Later downloads get the finished MP3.
- Only the `Pocket Casts` user agent can start synthesis. Pocket Casts' servers download every new episode as `WordPress.com - Audio`, and would otherwise synthesize everything.

Pocket Casts starts playback after buffering about 512 KB, so the first listen of an article takes 10 to 20 seconds to start. It imports at most 10 new episodes per feed refresh.

## Configuration

| Variable | Meaning |
|---|---|
| `GEMINI_API_KEY` | Gemini API key |
| `FEED_TOKEN` | Secret path segment; the feed is `https://<host>/<token>/feed.xml` |
| `STORE_DIR` | Directory for article text and audio |

Run locally with `uv run hypercorn app:app`.

## Deploy

Cloud Run, one instance, with a Cloud Storage bucket mounted at `/store`:

```bash
gcloud run deploy articlecast --source . --region us-west1 --use-http2 \
  --max-instances 1 --no-cpu-throttling --timeout 3600 --allow-unauthenticated \
  --add-volume name=store,type=cloud-storage,bucket=$BUCKET \
  --add-volume-mount volume=store,mount-path=/store \
  --set-env-vars STORE_DIR=/store \
  --set-secrets GEMINI_API_KEY=gemini-api-key:latest,FEED_TOKEN=feed-token:latest
```

One instance keeps every request for an episode on the same synthesis. CPU stays allocated so synthesis finishes after the phone disconnects. End-to-end HTTP/2, served by Hypercorn, is required: over HTTP/1 Cloud Run buffers any response that declares its size, and Pocket Casts only plays responses that do.
