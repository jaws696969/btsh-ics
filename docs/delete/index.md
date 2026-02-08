---
title: BTSH Calendars
---

# BTSH Calendars

Click a team to download/subscribe to its `.ics` feed.

> **Google Calendar:** Other calendars → **From URL** → paste the `.ics` link.

{% assign base = site.github.url | append: site.baseurl %}
{% capture teams %}{% include_relative index.txt %}{% endcapture %}
{% assign lines = teams | split: "
" %}

{%- comment -%}
Map BTSH season numbers to calendar year.
Update this if BTSH changes their numbering.
{%- endcomment -%}
{% assign season_year_map = "2:2025,3:2026,4:2027" | split: "," %}

{%- comment -%}
Collect unique years present in index.txt
{%- endcomment -%}
{% assign years = "" | split: "" %}

{% for line in lines %}
  {% assign cols = line | split: "	" %}
  {% assign team = cols[0] | strip %}
  {% assign file = cols[1] | strip %}
  {% if team != "" and file != "" %}
    {% assign season = file | split: "season-" | last | split: "." | first | strip %}
    {% assign year = season %}
    {% for kv in season_year_map %}
      {% assign pair = kv | split: ":" %}
      {% if pair[0] == season %}
        {% assign year = pair[1] %}
      {% endif %}
    {% endfor %}
    {% unless years contains year %}
      {% assign years = years | push: year %}
    {% endunless %}
  {% endif %}
{% endfor %}

{% assign years = years | sort | reverse %}

{% for y in years %}
## Season {{ y }}

<ul>
{% for line in lines %}
  {% assign cols = line | split: "	" %}
  {% assign team = cols[0] | strip %}
  {% assign file = cols[1] | strip %}
  {% if team != "" and file != "" %}
    {% assign season = file | split: "season-" | last | split: "." | first | strip %}
    {% assign year = season %}
    {% for kv in season_year_map %}
      {% assign pair = kv | split: ":" %}
      {% if pair[0] == season %}
        {% assign year = pair[1] %}
      {% endif %}
    {% endfor %}

    {% if year == y %}
      <li><a href="{{ base }}/{{ file }}">{{ team }}</a></li>
    {% endif %}
  {% endif %}
{% endfor %}
</ul>

{% endfor %}
