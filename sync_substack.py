# /// script
# requires-python = ">=3.12"
# ///
"""Keeps Pocket Casts subscribed to an Articlecast feed for each Substack publication the owner subscribes to
and each author they follow, leaving out authors of those publications.

Feeds without episodes are skipped until they have some. The sync records the Pocket Casts podcast of each
publication and author in a state file, whether it subscribed or found it subscribed, and unsubscribes it once
the publication or author leaves the owner's Substack lists; other podcasts are left alone.

Environment: ARTICLECAST_URL, FEED_TOKEN, SUBSTACK_SID (URL-decoded), POCKETCASTS_EMAIL, POCKETCASTS_PASSWORD.

Usage: uv run sync_substack.py <state file>
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

FEEDS = f"{os.environ['ARTICLECAST_URL']}/{os.environ['FEED_TOKEN']}/substack"
SUBSTACK_COOKIE = "substack.sid=" + urllib.parse.quote(os.environ["SUBSTACK_SID"], safe="")


def fetch(url, data=None, timeout=60, **headers):
    body = None if data is None else json.dumps(data).encode()
    if body:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, body, {"User-Agent": "Mozilla/5.0 (compatible; Articlecast)", **headers})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def substack(path):
    for _ in range(6):
        try:
            return json.loads(fetch(f"https://substack.com/api/v1/{path}", Cookie=SUBSTACK_COOKIE))
        except urllib.error.HTTPError as error:
            if error.code != 429:
                raise
            time.sleep(int(error.headers.get("Retry-After", 10)))
        except OSError:  # Substack also resets or stalls connections from clients that ask too fast
            time.sleep(30)
    raise RuntimeError(f"Substack's {path} still refuses requests")


def wanted_feeds():
    """Articlecast feed sources by publication or author: a publication's custom domain, if any, then its
    substack.com host, since some custom domains no longer resolve; an author's @handle, if known."""
    profile = substack("user/profile/self")
    if profile["subscriptionsTruncated"]:
        raise RuntimeError("Substack truncated the subscription list")
    feeds, authors = {}, {profile["id"]}
    for subscription in profile["subscriptions"]:
        publication = subscription["publication"]
        authors |= {publication["author_id"], publication["primary_user_id"]}
        feeds[f"publication:{publication['id']}"] = [host for host in (publication["custom_domain"], f"{publication['subdomain']}.substack.com") if host]
    for user_id in substack("feed/following"):
        if user_id in authors:
            continue
        # The handle comes from the byline of a recent post, so authors without one wait until they publish;
        # users who never chose a handle get no feed.
        posts = substack(f"profile/posts?profile_user_id={user_id}&limit=50")["posts"]
        feeds[f"user:{user_id}"] = [f"@{byline['handle']}" for post in posts for byline in post["publishedBylines"] if byline["id"] == user_id and byline["handle"]][:1]
    return feeds


def main():
    state_path = Path(sys.argv[1])
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    feeds = wanted_feeds()
    token = json.loads(fetch("https://api.pocketcasts.com/user/login", {
        "email": os.environ["POCKETCASTS_EMAIL"], "password": os.environ["POCKETCASTS_PASSWORD"], "scope": "webplayer"}))["token"]

    def pocketcasts(url, data):
        return json.loads(fetch(url, data, Authorization=f"Bearer {token}"))

    def podcast_of(url):
        """The Pocket Casts podcast of a feed URL, which Pocket Casts adds on first sight."""
        result = pocketcasts("https://refresh.pocketcasts.com/author/add_feed_url", {"url": url})
        while result["status"] == "poll":
            time.sleep(3)
            result = pocketcasts("https://refresh.pocketcasts.com/author/add_feed_url", {"poll_uuid": result["poll_uuid"]})
        if result["status"] != "ok":
            raise RuntimeError(f"Pocket Casts could not add {url}: {result['message']}")
        return result["result"]["podcast"]["uuid"]

    for key in state.keys() - feeds.keys():
        pocketcasts("https://api.pocketcasts.com/user/podcast/unsubscribe", {"uuid": state.pop(key)})
        state_path.write_text(json.dumps(state, indent=1))
        print(f"unsubscribed {key}", flush=True)
    subscribed = {podcast["uuid"] for podcast in pocketcasts("https://api.pocketcasts.com/user/podcast/list", {"v": 1})["podcasts"]}
    failed = []
    for key, sources in feeds.items():
        if key in state or not sources:
            continue
        for source in sources:
            url = f"{FEEDS}/{source}/feed.xml"
            try:
                xml = fetch(url, timeout=1800).decode()  # a first request reads the whole archive
            except OSError as error:
                print(f"{url} failed: {error}", flush=True)
                continue
            if "<item>" in xml:
                try:
                    state[key] = podcast_of(url)
                except RuntimeError as error:
                    print(error, flush=True)
                    continue
                if state[key] not in subscribed:
                    pocketcasts("https://api.pocketcasts.com/user/podcast/subscribe", {"uuid": state[key]})
                state_path.write_text(json.dumps(state, indent=1))
                print(f"{'kept' if state[key] in subscribed else 'subscribed to'} {source}", flush=True)
            break
        else:
            failed.append(key)
    if failed:
        raise SystemExit(f"no working feed for {', '.join(failed)}")


main()
