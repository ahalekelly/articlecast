# Substack API

Substack's web app calls an undocumented JSON API. These are the endpoints Articlecast and `sync_substack.py` use, plus account actions found the same way: by reading the web app's JavaScript bundles and checking responses.

## Signing in

Send the `substack.sid` cookie of a signed-in browser session. It covers `substack.com` and `*.substack.com`; each request extends it by 90 days. A publication on a custom domain needs its own session: requesting `https://substack.com/sign-in?redirect=%2F&for_pub=<subdomain>` with the cookie, following redirects, sets that domain's `connect.sid`.

## Reading

| Request | Returns |
|---|---|
| `GET substack.com/api/v1/user/profile/self` | The signed-in user, with `subscriptions`: each has `membership_state` (`subscribed` for paid, `free_signup`) and `publication` (`id`, `subdomain`, `custom_domain`, `author_id`) |
| `GET substack.com/api/v1/feed/following` | IDs of users the signed-in user follows, including themselves |
| `GET substack.com/api/v1/user/<handle>/public_profile` | A user's `id`, `name`, `bio`, `photo_url` |
| `GET substack.com/api/v1/profile/posts?profile_user_id=<id>&limit=50&next_cursor=<cursor>` | Posts the user wrote, posts in publications they own, and posts they restacked (`type` `restack`), newest first, with `nextCursor` |
| `GET <publication>/api/v1/archive?sort=new&offset=<n>&limit=50` | A publication's posts, newest first, without their text |
| `GET <publication>/api/v1/posts/<slug>` | One post with `body_html`; empty for paid posts unless signed in to a paid subscription |

Posts carry `audience` (`everyone` for free), `publication_id`, `publishedBylines`, and `audio_items`, whose completed `tts` item has an `audio_url` that downloads without signing in. Requests from one IP are rate limited with 429 and `Retry-After`; requests made too quickly also get their connections reset.

## Account actions

| Request | Effect |
|---|---|
| `POST substack.com/api/v1/feed/<user id>/follow` | Follow a user |
| `DELETE substack.com/api/v1/feed/<user id>/follow` | Unfollow a user |
| `DELETE <publication>/api/v1/free` with JSON `{"publication_id": <id>}` | Unsubscribe from a free subscription |

Account actions send the cookie and a JSON content type.
