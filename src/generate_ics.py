#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml
from zoneinfo import ZoneInfo


ASCII_RULE = "----------------------------------------"


def load_config(path: str = "config.yml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def http_get_json(url: str) -> Any:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ics_escape(text: str) -> str:
    # RFC5545 escaping: backslash, semicolon, comma, newline
    text = text.replace("\\", "\\\\")
    text = text.replace(";", r"\;")
    text = text.replace(",", r"\,")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", r"\n")
    return text


def fold_ics_line(line: str, limit: int = 75) -> str:
    # 75 octets is the spec; we do 75 chars (good enough for typical ASCII).
    if len(line) <= limit:
        return line
    out = []
    while len(line) > limit:
        out.append(line[:limit])
        line = " " + line[limit:]
    out.append(line)
    return "\r\n".join(out)


def fmt_dt_local(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S")


def ordinal_day(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def pretty_short_date(d: date) -> str:
    # "Feb 3rd"
    return f"{d.strftime('%b')} {ordinal_day(d.day)}"


def stable_calendar_id(team_id: int, season: int) -> str:
    return f"btsh-team-{team_id}-season-{season}"


def uid_for_event(kind: str, season: int, obj_id: int) -> str:
    # Stable UID so updates replace events in clients
    return f"btsh-{kind}-s{season}-{obj_id}@btsh"


def sha1_short(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def vtimezone_america_new_york() -> str:
    # Minimal VTIMEZONE block (works well enough in Google/Apple).
    # Note: This is a common canonical snippet for America/New_York DST rules.
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


def parse_time_hms(s: str) -> time:
    # "12:00:00"
    hh, mm, ss = s.split(":")
    return time(int(hh), int(mm), int(ss))


def parse_duration_hms(s: str) -> int:
    # "00:50:00" -> seconds
    hh, mm, ss = s.split(":")
    return int(hh) * 3600 + int(mm) * 60 + int(ss)


def is_tbd_team(team: Team) -> bool:
    # API placeholder uses "-" as name/short_name in your sample :contentReference[oaicite:3]{index=3}
    return team.name.strip() == "-" or team.short_name.strip() == "-"


def parse_game_day(obj: Dict[str, Any], tz: ZoneInfo) -> LeagueDay:
    day_str = obj["day"]
    d = datetime.strptime(day_str, "%Y-%m-%d").date()

    type_key = obj.get("type") or ""
    type_display = obj.get("get_type_display") or type_key or "Day"
    description = (obj.get("description") or "").strip()
    note = (obj.get("note") or "").strip()
    day_id = int(obj["id"])

    games: List[Game] = []
    for g in obj.get("games") or []:
        home_raw = g["home_team"]
        away_raw = g["away_team"]

        home = Team(
            id=int(home_raw["id"]),
            name=str(home_raw["name"]),
            short_name=str(home_raw.get("short_name") or home_raw["name"]),
        )
        away = Team(
            id=int(away_raw["id"]),
            name=str(away_raw["name"]),
            short_name=str(away_raw.get("short_name") or away_raw["name"]),
        )

        start_t = parse_time_hms(g["start"])
        dur_sec = parse_duration_hms(g.get("duration") or "00:50:00")
        end_t = parse_time_hms(g["end"]) if g.get("end") else (datetime.combine(d, start_t)  # type: ignore
                                                               .replace(tzinfo=tz)
                                                               .astimezone(tz)
                                                               .time())

        start_local = datetime.combine(d, start_t).replace(tzinfo=tz)
        # prefer explicit end if present; otherwise duration
        if g.get("end"):
            end_local = datetime.combine(d, end_t).replace(tzinfo=tz)
        else:
            end_local = start_local.fromtimestamp(start_local.timestamp() + dur_sec, tz=tz)

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
    for day in days:
        for g in day.games:
            teams[g.home.id] = g.home
            teams[g.away.id] = g.away
    return teams


def all_games(days: List[LeagueDay]) -> List[Game]:
    out: List[Game] = []
    for d in days:
        out.extend(d.games)
    out.sort(key=lambda x: (x.start_local, x.id))
    return out


def game_result_string(g: Game, team_id_for_perspective: Optional[int] = None) -> str:
    """
    Returns:
      - "(W 10-5)" or "(L 5-10)" if completed and scores exist
      - "(Cancelled)" if cancelled
      - "" if scheduled / missing scores
    """
    if g.status.lower() == "cancelled":
        return "(Cancelled)"

    if g.home_goals is None or g.away_goals is None:
        return ""

    # If we can determine W/L from perspective
    if team_id_for_perspective is not None:
        if team_id_for_perspective == g.home.id:
            w = g.home_goals > g.away_goals
            return f"({'W' if w else 'L'} {g.home_goals}-{g.away_goals})"
        if team_id_for_perspective == g.away.id:
            w = g.away_goals > g.home_goals
            return f"({'W' if w else 'L'} {g.away_goals}-{g.home_goals})"

    # Otherwise neutral score
    return f"({g.home_goals}-{g.away_goals})"


def opponent_recent_lines(
    opponent: Team,
    games_all: List[Game],
    event_start: datetime,
    limit: int,
) -> List[str]:
    # Include all opponent games with start < this event's start
    opp_games = [
        g
        for g in games_all
        if g.start_local < event_start and (g.home.id == opponent.id or g.away.id == opponent.id)
    ]
    opp_games.sort(key=lambda g: (g.start_local, g.id), reverse=True)
    opp_games = opp_games[:limit]
    opp_games.reverse()  # show oldest->newest

    lines: List[str] = []
    for g in opp_games:
        # identify opponent's opponent + home/away marker
        if g.home.id == opponent.id:
            other = g.away
            vs = "vs"
            res = game_result_string(g, team_id_for_perspective=opponent.id)
        else:
            other = g.home
            vs = "@"
            res = game_result_string(g, team_id_for_perspective=opponent.id)

        d = g.start_local.date()
        date_str = pretty_short_date(d)
        if res:
            lines.append(f"    {date_str} {vs} {other.name} {res}")
        else:
            # no result yet (or league hasn’t posted), still include the game
            # user requested: include game but don't list a result if missing
            lines.append(f"    {date_str} {vs} {other.name}")
    return lines


def prior_matchups_lines(
    my_team: Team,
    opp_team: Team,
    games_all: List[Game],
    event_start: datetime,
    limit: int = 10,
) -> List[str]:
    # Prior matchups strictly before event start
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
        d = g.start_local.date()
        date_str = pretty_short_date(d)

        # Perspective: my_team W/L if we have scores
        res = game_result_string(g, team_id_for_perspective=my_team.id)
        # show @/vs from my_team perspective
        if g.home.id == my_team.id:
            vs = "vs"
        else:
            vs = "@"
        if res:
            lines.append(f"    {date_str} {vs} {opp_team.name} {res}")
        else:
            lines.append(f"    {date_str} {vs} {opp_team.name}")
    return lines


def build_event_description(
    g: Game,
    my_team: Team,
    games_all: List[Game],
    opponent_recent_limit: int,
) -> str:
    # Who is opponent from my_team perspective?
    opp = g.away if g.home.id == my_team.id else g.home

    parts: List[str] = []

    # Top line: status / metadata
    parts.append(f"League: BTSH Season")
    parts.append(f"Status: {g.status_display}")
    if g.location:
        parts.append(f"Location: {g.location}" + (f" ({g.court})" if g.court else ""))
    elif g.court:
        parts.append(f"Court: {g.court}")
    if g.type_display:
        parts.append(f"Type: {g.type_display}")

    # If we have final score, include it (neutral, plus perspective)
    if g.status.lower() == "completed" and g.home_goals is not None and g.away_goals is not None:
        parts.append("")
        parts.append("Result:")
        parts.append(f"  {g.home.name} {g.home_goals} - {g.away_goals} {g.away.name}")

    # Opponent recent games (before this matchup)
    parts.append("")
    parts.append(ASCII_RULE)
    parts.append(f"{opp.name} games (before this matchup):")
    opp_lines = opponent_recent_lines(opp, games_all, g.start_local, opponent_recent_limit)
    if opp_lines:
        parts.extend(opp_lines)
    else:
        parts.append("    (no prior games listed)")

    # Prior matchups between my team and opponent
    parts.append("")
    parts.append(ASCII_RULE)
    parts.append(f"Prior matchups: {my_team.name} vs {opp.name} (before this matchup):")
    matchup_lines = prior_matchups_lines(my_team, opp, games_all, g.start_local, limit=opponent_recent_limit)
    if matchup_lines:
        parts.extend(matchup_lines)
    else:
        parts.append("    (no prior matchups listed)")

    return "\n".join(parts).strip()


def summary_for_team_game(g: Game, my_team: Team) -> str:
    # Title: "My Team vs Opp" or "My Team @ Opp" plus cancellation tag
    if g.home.id == my_team.id:
        opp = g.away
        vs = "vs"
    else:
        opp = g.home
        vs = "@"

    prefix = ""
    if g.status.lower() == "cancelled":
        prefix = "[CANCELLED] "

    return f"{prefix}{my_team.name} {vs} {opp.name}"


def summary_for_neutral_game(g: Game) -> str:
    prefix = "[CANCELLED] " if g.status.lower() == "cancelled" else ""
    return f"{prefix}{g.home.name} vs {g.away.name}"


def build_league_wide_day_event(day: LeagueDay) -> Tuple[str, str]:
    # All-day event summary + description for placeholders like make_up/holiday/other with no games
    title_bits = []
    if day.type_display:
        title_bits.append(day.type_display)
    if day.description:
        title_bits.append(day.description)
    title = " - ".join(title_bits) if title_bits else "League Day"

    desc_parts = []
    if day.description:
        desc_parts.append(day.description)
    if day.note:
        desc_parts.append(day.note)
    if not desc_parts:
        desc_parts.append("League-wide placeholder / non-game day.")
    desc = "\n".join(desc_parts)
    return title, desc


def ics_calendar(
    tzid: str,
    cal_name: str,
    events: List[str],
) -> str:
    lines = []
    lines.append("BEGIN:VCALENDAR")
    lines.append("VERSION:2.0")
    lines.append("PRODID:-//BTSH ICS//EN")
    lines.append("CALSCALE:GREGORIAN")
    lines.append(f"X-WR-CALNAME:{ics_escape(cal_name)}")
    lines.append(f"X-WR-TIMEZONE:{tzid}")
    lines.append(vtimezone_america_new_york())
    lines.extend(events)
    lines.append("END:VCALENDAR")

    folded = "\r\n".join(fold_ics_line(l) for l in lines) + "\r\n"
    return folded


def ics_event_block(
    uid: str,
    dtstamp_utc: datetime,
    summary: str,
    dtstart: datetime,
    dtend: datetime,
    tzid: str,
    description: str,
    location: str = "",
    status: Optional[str] = None,  # "CANCELLED" if you want; but we keep visible label + optional STATUS
) -> str:
    lines = []
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
    # Optional STATUS:CANCELLED (some clients hide it; but you asked to include them, so we keep SUMMARY label)
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
    # All-day uses VALUE=DATE and DTEND is next day per ICS
    d0 = day_date.strftime("%Y%m%d")
    d1 = (day_date.toordinal() + 1)
    next_day = date.fromordinal(d1).strftime("%Y%m%d")

    lines = []
    lines.append("BEGIN:VEVENT")
    lines.append(f"UID:{uid}")
    lines.append(f"DTSTAMP:{dtstamp_utc.strftime('%Y%m%dT%H%M%SZ')}")
    lines.append(f"SUMMARY:{ics_escape(summary)}")
    lines.append(f"DTSTART;VALUE=DATE:{d0}")
    lines.append(f"DTEND;VALUE=DATE:{next_day}")
    if description:
        lines.append(f"DESCRIPTION:{ics_escape(description)}")
    lines.append("END:VEVENT")
    return "\r\n".join(fold_ics_line(l) for l in lines)


def main() -> None:
    cfg = load_config()

    output_dir = cfg.get("output_dir", "docs")
    tzid = cfg.get("default_timezone", "America/New_York")
    tz = ZoneInfo(tzid)

    season = int(cfg["season"])
    api_url = str(cfg["api_url"]).format(season=season)

    opponent_recent_limit = int(cfg.get("opponent_recent_limit", 10))
    include_league_wide_days = bool(cfg.get("include_league_wide_days", True))
    include_tbd_games_on_all_calendars = bool(cfg.get("include_tbd_games_on_all_calendars", True))

    raw = http_get_json(api_url)

    # raw shape in your sample is a dict with "results": [...] :contentReference[oaicite:4]{index=4}
    day_objs = raw["results"] if isinstance(raw, dict) and "results" in raw else raw

    days: List[LeagueDay] = [parse_game_day(obj, tz) for obj in day_objs]
    days.sort(key=lambda d: (d.day, d.id))

    teams_by_id = build_team_index(days)
    games_all_list = all_games(days)

    now_utc = datetime.utcnow()

    os.makedirs(output_dir, exist_ok=True)

    # Build per-team event lists
    team_events: Dict[int, List[str]] = {tid: [] for tid in teams_by_id.keys()}

    # League-wide events (applied to all)
    league_wide_event_blocks: List[str] = []
    if include_league_wide_days:
        for d in days:
            if d.games:
                continue
            # Only include meaningful placeholders / non-game days
            # Sample includes holiday/make_up/other with games: [] :contentReference[oaicite:5]{index=5} :contentReference[oaicite:6]{index=6}
            title, desc = build_league_wide_day_event(d)
            uid = uid_for_event("day", season, d.id)
            league_wide_event_blocks.append(
                ics_all_day_event_block(
                    uid=uid,
                    dtstamp_utc=now_utc,
                    summary=f"[PLACEHOLDER] {title}",
                    day_date=d.day,
                    description=desc,
                )
            )

    # Game events
    for g in games_all_list:
        home_is_tbd = is_tbd_team(g.home)
        away_is_tbd = is_tbd_team(g.away)
        is_tbd = home_is_tbd or away_is_tbd

        # Determine which calendars get this game
        target_team_ids: List[int] = []
        if not home_is_tbd:
            target_team_ids.append(g.home.id)
        if not away_is_tbd and g.away.id not in target_team_ids:
            target_team_ids.append(g.away.id)

        # If TBD playoff/placeholder game, optionally include on every calendar
        if is_tbd and include_tbd_games_on_all_calendars:
            target_team_ids = list(teams_by_id.keys())

        # If still no targets, skip
        if not target_team_ids:
            continue

        # Location string
        loc = g.location
        if g.court:
            loc = f"{loc} ({g.court})" if loc else g.court

        # Build event blocks tailored for each team (different SUMMARY + description content)
        for tid in target_team_ids:
            my_team = teams_by_id.get(tid)
            if my_team is None:
                continue

            uid = uid_for_event("game", season, g.id)

            # For TBD games, use neutral title/description
            if is_tbd:
                summary = summary_for_neutral_game(g)
                desc = "\n".join(
                    [
                        "Playoff / placeholder game (teams TBD).",
                        f"Status: {g.status_display}",
                        (f"Location: {loc}" if loc else ""),
                    ]
                ).strip()
            else:
                summary = summary_for_team_game(g, my_team)
                desc = build_event_description(g, my_team, games_all_list, opponent_recent_limit)

            # Keep cancelled games visible:
            # - Label in SUMMARY
            # - Also set STATUS:CANCELLED (optional; some clients style it)
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

    # Write calendars
    for tid, team in teams_by_id.items():
        events = []
        events.extend(league_wide_event_blocks)
        events.extend(team_events.get(tid, []))

        cal_name = f"BTSH — {team.name} (Season {season})"
        ics = ics_calendar(tzid=tzid, cal_name=cal_name, events=events)

        safe_slug = re.sub(r"[^a-z0-9]+", "-", team.name.lower()).strip("-")
        filename = f"{safe_slug}-season-{season}.ics"
        out_path = os.path.join(output_dir, filename)

        with open(out_path, "w", encoding="utf-8", newline="") as f:
            f.write(ics)

    # Optional: write an index file for convenience
    index_path = os.path.join(output_dir, "index.txt")
    with open(index_path, "w", encoding="utf-8") as f:
        for tid, team in sorted(teams_by_id.items(), key=lambda kv: kv[1].name.lower()):
            safe_slug = re.sub(r"[^a-z0-9]+", "-", team.name.lower()).strip("-")
            filename = f"{safe_slug}-season-{season}.ics"
            f.write(f"{team.name}\t{filename}\n")

    print(f"Wrote {len(teams_by_id)} team calendars to: {output_dir}/")


if __name__ == "__main__":
    main()
