"""RSS for a podcast feed."""

from email.utils import formatdate
from xml.sax.saxutils import escape


def feed_xml(title, link, description, author, image, items):
    """Each item has a title, description, link, guid, published time, and an enclosure url, size, and duration."""
    episodes = "".join(f"""
    <item>
      <title>{escape(i["title"])}</title>
      <description>{escape(i["description"])}</description>
      <link>{escape(i["link"])}</link>
      <guid isPermaLink="false">{escape(i["guid"])}</guid>
      <pubDate>{formatdate(i["published"], usegmt=True)}</pubDate>
      <enclosure url="{escape(i["url"])}" length="{i["size"]}" type="audio/mpeg"/>
      <itunes:duration>{i["duration"]}</itunes:duration>
    </item>""" for i in items)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>{escape(title)}</title>
    <link>{escape(link)}</link>
    <description>{escape(description)}</description>
    <language>en-us</language>
    <itunes:author>{escape(author)}</itunes:author>{f'''
    <itunes:image href="{escape(image)}"/>''' if image else ""}
    <itunes:explicit>false</itunes:explicit>{episodes}
  </channel>
</rss>
"""
