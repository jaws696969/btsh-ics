---
title: BTSH Calendars
---

# BTSH Calendars

Click a team to download/subscribe to its `.ics` feed.

> Tip: In Google Calendar: **Other calendars → From URL** and paste the `.ics` link.

{% assign base = site.github.url | append: site.baseurl %}

{% capture teams %}{% include_relative index.txt %}{% endcapture %}
{% assign lines = teams | split: "
" %}

<ul>
{% for line in lines %}
  {% assign cols = line | split: "	" %}
  {% assign team = cols[0] %}
  {% assign file = cols[1] %}
  {% if team and file %}
    <li><a href="{{ base }}/{{ file | strip }}">{{ team }}</a></li>
  {% endif %}
{% endfor %}
</ul>
