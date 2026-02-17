#!/usr/bin/env python3
"""
BTSH Team Calendar Generator (one .ics per team)

Features (v1):
- Builds schedule for every team in a given season
- Updates results on subsequent runs (stable UIDs)
- Includes cancelled games (labeled [CANCELLED] + STATUS:CANCELLED)
- Includes league-wide placeholder days (game_days with no games) on every team calendar
- Includes opponent games-to-date in each game event description
  - includes all opponent games with start < this event start
  - includes results when posted; omits when missing
- Includes head-to-head prior matchups only
- Includes opponent record-to-date (completed games with posted scores only)
- Includes BTSH check-in link in each event

Config:
- Looks for config.yml in repo root
- Required: season, api_url (or uses default)
- Optional: output_dir, default_timezone, opponent_recent_limit, league_name, checkin_url, include_league_wide_days, include_tbd_games_on_all_calendars

Dependencies:
  pip install requests pyyaml
"""

from __future__ import annotations

import os
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml
from zoneinfo import ZoneInfo

ASCII_RULE = "----------------------------------------"


# ----------------------------
# Config / HTTP
# ----------------------------
def load_config(path: str = "config.yml") -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def http_get_json(url: str) -> Any:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ----------------------------
# ICS helpers
# ----------------------------
def ics_escape(text: str) -> str:
    # RFC5545 escaping: backslash, semicolon, comma, newline
    text = text.replace("\\", "\\\\")
    text = text.replace(";", r"\;")
    text = text.replace(",", r"\,")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", r"\n")
    return text


def fold_ics_line(line: str, limit: int = 75) -> str:
    # Spec is 75 octets; we do 75 chars which is fine for typical ASCII text.
    if len(line) <= limit:
        return line
    out = []
    while len(line) > limit:
        out.append(line[:limit])
        line = " " + line[limit:]
    out.append(line)
    return "\r\n".join(out)


def fmt_dt_local(dt: datetime) -> str:
    # DTSTART;TZID=... expects local-time value without Z
    return dt.strftime("%Y%m%dT%H%M%S")


def vtimezone_america_new_york() -> str:
    # Minimal VTIMEZONE that works well in Google/Apple clients.
    return "\r\n".join(
        [
            "BEGIN:VTIMEZONE",
            "TZID:America/New_York",
            "X-LIC-LOCATION:America/New_York",
            "BEGIN:DAYLIGHT",
            "TZOFFSETFROM:-0500",
            "TZOFFSETTO:-0400",
            "TZNAME:EDT",
            "DTSTART:19700308T020000",
            "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
            "END:DAYLIGHT",
            "BEGIN:STANDARD",
            "TZOFFSETFROM:-0400",
            "TZOFFSETTO:-0500",
            "TZNAME:EST",
            "DTSTART:19701101T020000",
            "RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
            "END:STANDARD",
            "END:VTIMEZONE",
        ]
    )


def ics_calendar(tzid: str, cal_name: str, events: List[str]) -> str:
    lines: List[str] = []
    lines.append("BEGIN:VCALENDAR")
    lines.append("VERSION:2.0")
    lines.append("PRODID:-//BTSH ICS//EN")
    lines.append("CALSCALE:GREGORIAN")
    lines.append(f"X-WR-CALNAME:{ics_escape(cal_name)}")
    lines.append(f"X-WR-TIMEZONE:{tzid}")
    if tzid == "America/New_York":
        lines.append(vtimezone_america_new_york())
    lines.extend(events)
    lines.append("END:VCALENDAR")
    return "\r\n".join(fold_ics_line(l) for l in lines) + "\r\n"


def ics_event_block(
    uid: str,
    dtstamp_utc: datetime,
    summary: str,
    dtstart: datetime,
    dtend: datetime,
    tzid: str,
    description: str,
    location: str = "",
    status: Optional[str] = None,
) -> str:
    lines: List[str] = []
    lines.append("BEGIN:VEVENT")
    lines.append(f"UID:{uid}")
    lines.append(f"DTSTAMP:{dtstamp_utc.strftime('%Y%m%dT%H%M%SZ')}")
    lines.append(f"SUMMARY:{ics_escape(summary)}")
    lines.append(f"DTSTART;TZID={tzid}:{fmt_dt_local(dtstart)}")
    lines.append(f"DTEND;TZID={tzid}:{fmt_dt_local(dtend)}")
    if location:
        lines.append(f"LOCATION:{ics_escape(location)}")
    if description:
        lines.append(f"DESCRIPTION:{ics_escape(description)}")
    if status:
        lines.append(f"STATUS:{status}")
    lines.append("END:VEVENT")
    return "\r\n".join(fold_ics_line(l) for l in lines)


def ics_all_day_event_block(
    uid: str,
    dtstamp_utc: datetime,
    summary: str,
    day_date: date,
    description: str,
) -> str:
    # All-day uses VALUE=DATE; DTEND is next day per ICS
    d0 = day_date.strftime("%Y%m%d")
    d1 = (day_date + timedelta(days=1)).strftime("%Y%m%d")

    lines: List[str] = []
    lines.append("BEGIN:VEVENT")
    lines.append(f"UID:{uid}")
    lines.append(f"DTSTAMP:{dtstamp_utc.strftime('%Y%m%dT%H%M%SZ')}")
    lines.append(f"SUMMARY:{ics_escape(summary)}")
    lines.append(f"DTSTART;VALUE=DATE:{d0}")
    lines.append(f"DTEND;VALUE=DATE:{d1}")
    if description:
        lines.append(f"DESCRIPTION:{ics_escape(description)}")
    lines.append("END:VEVENT")
    return "\r\n".join(fold_ics_line(l) for l in lines)


# ----------------------------
# Domain model
# ----------------------------
@dataclass(frozen=True)
class Team:
    id: int
    name: str
    short_name: str


@dataclass(frozen=True)
class Game:
    id: int
    day: date
    start_local: datetime
    end_local: datetime
    home: Team
    away: Team
    location: str
    court: str
    type_display: str
    status: str
    status_display: str
    home_goals: Optional[int]
    away_goals: Optional[int]


@dataclass(frozen=True)
class LeagueDay:
    id: int
    day: date
    type_key: str
    type_display: str
    description: str
    note: str
    games: List[Game]


# ----------------------------
# Parsing
# ----------------------------
def parse_time_hms(s: str) -> time:
    hh, mm, ss = s.split(":")
    return time(int(hh), int(mm), int(ss))


def parse_duration_hms(s: str) -> int:
    hh, mm, ss = s.split(":")
    return int(hh) * 3600 + int(mm) * 60 + int(ss)


def parse_game_day(obj: Dict[str, Any], tz: ZoneInfo) -> LeagueDay:
    d = datetime.strptime(obj["day"], "%Y-%m-%d").date()

    type_key = (obj.get("type") or "").strip()
    type_display = (obj.get("get_type_display") or type_key or "Day").strip()
    description = (obj.get("description") or "").strip()
    note = (obj.get("note") or "").strip()
    day_id = int(obj["id"])

    games: List[Game] = []
    for g in (obj.get("games") or []):
        home_raw = g["home_team"]
        away_raw = g["away_team"]

        home = Team(
            id=int(home_raw["id"]),
            name=str(home_raw.get("name") or "").strip(),
            short_name=str(home_raw.get("short_name") or home_raw.get("name") or "").strip(),
        )
        away = Team(
            id=int(away_raw["id"]),
            name=str(away_raw.get("name") or "").strip(),
            short_name=str(away_raw.get("short_name") or away_raw.get("name") or "").strip(),
        )

        start_t = parse_time_hms(g["start"])
        dur_sec = parse_duration_hms(g.get("duration") or "00:50:00")

        start_local = datetime.combine(d, start_t).replace(tzinfo=tz)

        if g.get("end"):
            end_t = parse_time_hms(g["end"])
            end_local = datetime.combine(d, end_t).replace(tzinfo=tz)
        else:
            end_local = start_local + timedelta(seconds=dur_sec)

        games.append(
            Game(
                id=int(g["id"]),
                day=d,
                start_local=start_local,
                end_local=end_local,
                home=home,
                away=away,
                location=str(g.get("location") or "").strip(),
                court=str(g.get("get_court_display") or g.get("court") or "").strip(),
                type_display=str(g.get("get_type_display") or g.get("type") or "").strip(),
                status=str(g.get("status") or "").strip(),
                status_display=str(g.get("get_status_display") or g.get("status") or "").strip(),
                home_goals=g.get("home_team_num_goals"),
                away_goals=g.get("away_team_num_goals"),
            )
        )

    return LeagueDay(
        id=day_id,
        day=d,
        type_key=type_key,
        type_display=type_display,
        description=description,
        note=note,
        games=games,
    )


def build_team_index(days: List[LeagueDay]) -> Dict[int, Team]:
    teams: Dict[int, Team] = {}
    for d in days:
        for g in d.games:
            teams[g.home.id] = g.home
            teams[g.away.id] = g.away
    return teams


def all_games(days: List[LeagueDay]) -> List[Game]:
    out: List[Game] = []
    for d in days:
        out.extend(d.games)
    out.sort(key=lambda x: (x.start_local, x.id))
    return out


# ----------------------------
# Formatting helpers
# ----------------------------
def ordinal_day(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def pretty_short_date(d: date) -> str:
    return f"{d.strftime('%b')} {ordinal_day(d.day)}"


def format_local_dt_line(dt: datetime) -> str:
    # Example: 2026-03-26 20:30 EDT
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def is_tbd_team(team: Team) -> bool:
    return team.name.strip() == "-" or team.short_name.strip() == "-"


def uid_for_event(kind: str, season: int, obj_id: int) -> str:
    # Stable UID ensures updates overwrite rather than duplicate in clients
    return f"btsh-{kind}-s{season}-{obj_id}@btsh"


def safe_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


# ----------------------------
# Game result / records
# ----------------------------
def game_result_string(g: Game, team_id_for_perspective: Optional[int] = None) -> str:
    """
    Returns a compact result without parentheses:
      - "W 10-5" / "L 5-10" from perspective if scores exist
      - "Cancelled" if cancelled
      - "" if no posted scores yet
    """
    if g.status.lower() == "cancelled":
        return "Cancelled"

    if g.home_goals is None or g.away_goals is None:
        return ""

    if team_id_for_perspective is not None:
        if team_id_for_perspective == g.home.id:
            w = g.home_goals > g.away_goals
            return f"{'W' if w else 'L'} {g.home_goals}-{g.away_goals}"
        if team_id_for_perspective == g.away.id:
            w = g.away_goals > g.home_goals
            return f"{'W' if w else 'L'} {g.away_goals}-{g.home_goals}"

    return f"{g.home_goals}-{g.away_goals}"


def team_record_to_date(team: Team, games_all: List[Game], cutoff: datetime) -> str:
    """
    Record based only on games:
    - with posted scores
    - status == completed
    - start < cutoff
    """
    w = l = t = 0
    for g in games_all:
        if not (g.start_local < cutoff and (g.home.id == team.id or g.away.id == team.id)):
            continue
        if g.home_goals is None or g.away_goals is None:
            continue
        if g.status.lower() != "completed":
            continue

        team_goals = g.home_goals if g.home.id == team.id else g.away_goals
        opp_goals = g.away_goals if g.home.id == team.id else g.home_goals

        if team_goals > opp_goals:
            w += 1
        elif team_goals < opp_goals:
            l += 1
        else:
            t += 1

    return f"{w}-{l}-{t}" if t > 0 else f"{w}-{l}"


# ----------------------------
# Lines for description
# ----------------------------
def opponent_games_to_date_lines(
    opponent: Team,
    games_all: List[Game],
    event_start: datetime,
    limit: int,
) -> List[str]:
    # All opponent games with start < event_start
    opp_games = [
        g
        for g in games_all
        if g.start_local < event_start and (g.home.id == opponent.id or g.away.id == opponent.id)
    ]
    opp_games.sort(key=lambda g: (g.start_local, g.id), reverse=True)
    opp_games = opp_games[:limit]
    opp_games.reverse()

    lines: List[str] = []
    for g in opp_games:
        if g.home.id == opponent.id:
            other = g.away
            side = "vs"
            res = game_result_string(g, team_id_for_perspective=opponent.id)
        else:
            other = g.home
            side = "@"
            res = game_result_string(g, team_id_for_perspective=opponent.id)

        d = g.start_local.date()
        date_str = pretty_short_date(d)

        if res:
            lines.append(f"    {date_str} {side} {other.name} ({res})")
        else:
            lines.append(f"    {date_str} {side} {other.name}")

    return lines


def head_to_head_lines(
    my_team: Team,
    opp_team: Team,
    games_all: List[Game],
    event_start: datetime,
    limit: int,
) -> List[str]:
    # Prior matchups only
    matchups = [
        g
        for g in games_all
        if g.start_local < event_start
        and (
            (g.home.id == my_team.id and g.away.id == opp_team.id)
            or (g.home.id == opp_team.id and g.away.id == my_team.id)
        )
    ]
    matchups.sort(key=lambda g: (g.start_local, g.id), reverse=True)
    matchups = matchups[:limit]
    matchups.reverse()

    lines: List[str] = []
    for g in matchups:
        date_str = pretty_short_date(g.start_local.date())

        # show @/vs from my team's perspective
        side = "vs" if g.home.id == my_team.id else "@"

        res = game_result_string(g, team_id_for_perspective=my_team.id)
        if res:
            lines.append(f"    {date_str} {side} {opp_team.name} ({res})")
        else:
            lines.append(f"    {date_str} {side} {opp_team.name}")
    return lines


def build_event_description(
    g: Game,
    my_team: Team,
    games_all: List[Game],
    league_label: str,
    checkin_url: str,
    tzid: str,
    opponent_recent_limit: int,
) -> str:
    opp = g.away if g.home.id == my_team.id else g.home

    # Build rink/location line
    rink_line = ""
    if g.location:
        rink_line = f"Rink: {g.location}"
        if g.court:
            rink_line += f" ({g.court})"
    elif g.court:
        rink_line = f"Court: {g.court}"

    parts: List[str] = []

    # GAME INFO
    parts.append(ASCII_RULE)
    parts.append("GAME INFO")
    parts.append(ASCII_RULE)
    parts.append(f"League: {league_label}")
    if g.type_display:
        parts.append(f"Stage: {g.type_display}")
    parts.append(f"Status: {g.status}")
    parts.append(f"Start ({tzid}): {format_local_dt_line(g.start_local)}")
    if rink_line:
        parts.append(rink_line)
    parts.append(f"Check-in / game registration: {checkin_url}")

    # If completed with score, add final line
    if g.status.lower() == "completed" and g.home_goals is not None and g.away_goals is not None:
        parts.append("")
        parts.append(f"Final: {g.home.name} {g.home_goals}-{g.away_goals} {g.away.name}")

    # HEAD-TO-HEAD
    parts.append("")
    parts.append(ASCII_RULE)
    parts.append(f"HEAD-TO-HEAD vs {opp.name}")
    parts.append(ASCII_RULE)

    h2h = head_to_head_lines(my_team, opp, games_all, g.start_local, limit=opponent_recent_limit)
    if h2h:
        parts.extend(h2h)
    else:
        parts.append("    (no prior matchups listed)")

    # OPPONENT GAMES-TO-DATE
    parts.append("")
    parts.append(ASCII_RULE)
    parts.append(f"{opp.name.upper()} GAMES-TO-DATE")
    parts.append(ASCII_RULE)

    record = team_record_to_date(opp, games_all, cutoff=g.start_local)
    parts.append(f"Record to date (completed games only): {record}")

    opp_lines = opponent_games_to_date_lines(opp, games_all, g.start_local, limit=opponent_recent_limit)
    if opp_lines:
        parts.extend(opp_lines)
    else:
        parts.append("    (no prior games listed)")

    return "\n".join(parts)


def build_league_wide_day_event(day: LeagueDay) -> Tuple[str, str]:
    title_bits: List[str] = []
    if day.type_display:
        title_bits.append(day.type_display)
    if day.description:
        title_bits.append(day.description)
    title = " - ".join(title_bits) if title_bits else "League Day"

    desc_bits: List[str] = []
    if day.description:
        desc_bits.append(day.description)
    if day.note:
        desc_bits.append(day.note)
    if not desc_bits:
        desc_bits.append("League-wide placeholder / non-game day.")
    return title, "\n".join(desc_bits)


def summary_for_team_game(g: Game, my_team: Team) -> str:
    if g.home.id == my_team.id:
        opp = g.away
        side = "vs"
    else:
        opp = g.home
        side = "@"

    prefix = "[CANCELLED] " if g.status.lower() == "cancelled" else ""
    return f"{prefix}{my_team.name} {side} {opp.name}"


def summary_for_neutral_game(g: Game) -> str:
    prefix = "[CANCELLED] " if g.status.lower() == "cancelled" else ""
    return f"{prefix}{g.home.name} vs {g.away.name}"


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    cfg = load_config("config.yml")

    output_dir = str(cfg.get("output_dir", "docs"))
    tzid = str(cfg.get("default_timezone", "America/New_York"))
    tz = ZoneInfo(tzid)

    season = int(cfg.get("season", 2))
    api_url_tpl = str(cfg.get("api_url", "https://api.btsh.org/api/game_days/?season={season}"))
    api_url = api_url_tpl.format(season=season)

    opponent_recent_limit = int(cfg.get("opponent_recent_limit", 10))
    include_league_wide_days = bool(cfg.get("include_league_wide_days", True))
    include_tbd_games_on_all_calendars = bool(cfg.get("include_tbd_games_on_all_calendars", True))

    league_label = str(cfg.get("league_name", f"BTSH Season {season}"))
    checkin_url = str(cfg.get("checkin_url", "https://btsh.org/attendance"))

    raw = http_get_json(api_url)
    day_objs = raw["results"] if isinstance(raw, dict) and "results" in raw else raw

    days: List[LeagueDay] = [parse_game_day(obj, tz) for obj in day_objs]
    days.sort(key=lambda d: (d.day, d.id))

    teams_by_id = build_team_index(days)
    games_all_list = all_games(days)

    os.makedirs(output_dir, exist_ok=True)
    now_utc = datetime.utcnow()

    # Build league-wide placeholder blocks once
    league_wide_event_blocks: List[str] = []
    if include_league_wide_days:
        for d in days:
            if d.games:
                continue
            title, desc = build_league_wide_day_event(d)
            league_wide_event_blocks.append(
                ics_all_day_event_block(
                    uid=uid_for_event("day", season, d.id),
                    dtstamp_utc=now_utc,
                    summary=f"[PLACEHOLDER] {title}",
                    day_date=d.day,
                    description=desc,
                )
            )

    # Prepare per-team event lists
    team_events: Dict[int, List[str]] = {tid: [] for tid in teams_by_id.keys()}

    for g in games_all_list:
        home_tbd = is_tbd_team(g.home)
        away_tbd = is_tbd_team(g.away)
        is_tbd = home_tbd or away_tbd

        # Determine which team calendars get this game
        target_team_ids: List[int] = []
        if not home_tbd:
            target_team_ids.append(g.home.id)
        if not away_tbd and g.away.id not in target_team_ids:
            target_team_ids.append(g.away.id)

        if is_tbd and include_tbd_games_on_all_calendars:
            target_team_ids = list(teams_by_id.keys())

        if not target_team_ids:
            continue

        # location string
        loc = g.location
        if g.court:
            loc = f"{loc} ({g.court})" if loc else g.court

        for tid in target_team_ids:
            my_team = teams_by_id.get(tid)
            if my_team is None:
                continue

            uid = uid_for_event("game", season, g.id)

            if is_tbd:
                summary = summary_for_neutral_game(g)
                desc = "\n".join(
                    [
                        ASCII_RULE,
                        "GAME INFO",
                        ASCII_RULE,
                        "Playoff / placeholder game (teams TBD).",
                        f"Status: {g.status_display}",
                        (f"Location: {loc}" if loc else ""),
                        f"Check-in / game registration: {checkin_url}",
                    ]
                ).strip()
            else:
                summary = summary_for_team_game(g, my_team)
                desc = build_event_description(
                    g=g,
                    my_team=my_team,
                    games_all=games_all_list,
                    league_label=league_label,
                    checkin_url=checkin_url,
                    tzid=tzid,
                    opponent_recent_limit=opponent_recent_limit,
                )

            status_field = "CANCELLED" if g.status.lower() == "cancelled" else None

            team_events[tid].append(
                ics_event_block(
                    uid=uid,
                    dtstamp_utc=now_utc,
                    summary=summary,
                    dtstart=g.start_local,
                    dtend=g.end_local,
                    tzid=tzid,
                    description=desc,
                    location=loc,
                    status=status_field,
                )
            )

    # Write one calendar per team
    for tid, team in sorted(teams_by_id.items(), key=lambda kv: kv[1].name.lower()):
        events: List[str] = []
        events.extend(league_wide_event_blocks)
        events.extend(team_events.get(tid, []))

        cal_name = f"BTSH — {team.name} (Season {season})"
        ics = ics_calendar(tzid=tzid, cal_name=cal_name, events=events)

        filename = f"{safe_slug(team.name)}-season-{season}.ics"
        out_path = os.path.join(output_dir, filename)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            f.write(ics)

    # Simple index
    with open(os.path.join(output_dir, "index.txt"), "w", encoding="utf-8") as f:
        for tid, team in sorted(teams_by_id.items(), key=lambda kv: kv[1].name.lower()):
            filename = f"{safe_slug(team.name)}-season-{season}.ics"
            f.write(f"{team.name}\t{filename}\n")

    print(f"Wrote {len(teams_by_id)} team calendars to: {output_dir}/")


if __name__ == "__main__":
    main()
