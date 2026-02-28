#!/usr/bin/env python3
"""
BTSH ICS Generator

Fixes:
- BTSH game_days API returns day="YYYY-MM-DD" at the day level
  and start/end are often time-only strings like "13:00:00".
  We must combine them into full datetimes to produce valid VEVENTs.
- VEVENTs must include DTSTART (and typically DTEND) for calendar apps to accept them.

Outputs:
- One ICS per team for the configured season/year
- Optional league-wide / placeholder events included per config
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date, time
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml


# -----------------------------
# Config
# -----------------------------

@dataclass
class Config:
    output_dir: str = "docs"
    default_timezone: str = "America/New_York"

    # Either season id directly, or year (preferred). If year is set, we look up season id.
    season: Optional[int] = None
    year: Optional[int] = None

    api_url: str = "https://api.btsh.org/api/game_days/?season={season}"

    opponent_recent_limit: int = 10
    include_league_wide_days: bool = True
    include_tbd_games_on_all_calendars: bool = True

    # Optional: link to check-in / registration
    registration_url: str = "https://btsh.org"


def read_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    cfg = Config(**raw)
    return cfg


# -----------------------------
# Helpers (safe dict access)
# -----------------------------

def pick(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def to_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


# -----------------------------
# ICS helpers (escape + fold)
# -----------------------------

def ics_escape(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\\", "\\\\")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\n", "\\n")
    s = s.replace(",", "\\,")
    s = s.replace(";", "\\;")
    return s


def fold_ics_line(line: str, limit: int = 75) -> List[str]:
    """
    RFC5545 line folding: lines > 75 octets should be folded with CRLF + SPACE.
    We'll approximate by character count (good enough for ASCII-heavy content).
    """
    if len(line) <= limit:
        return [line]
    out = []
    cur = line
    while len(cur) > limit:
        out.append(cur[:limit])
        cur = " " + cur[limit:]
    out.append(cur)
    return out


def dt_local_ics(dt_local: datetime, tz_name: str) -> str:
    """
    For DTSTART;TZID=...:YYYYMMDDTHHMMSS
    dt_local must be tz-aware in tz_name.
    """
    if dt_local.tzinfo is None:
        dt_local = dt_local.replace(tzinfo=ZoneInfo(tz_name))
    dt_local = dt_local.astimezone(ZoneInfo(tz_name))
    return dt_local.strftime("%Y%m%dT%H%M%S")


def ics_event(
    uid: str,
    summary: str,
    dtstart_local: Optional[datetime],
    dtend_local: Optional[datetime],
    tz_name: str,
    description_lines: List[str],
    location: str = "",
    url: str = "",
) -> List[str]:
    lines: List[str] = ["BEGIN:VEVENT"]
    lines += [f"UID:{uid}"]
    lines += [f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"]
    lines += [f"SUMMARY:{ics_escape(summary)}"]

    # IMPORTANT: most clients require DTSTART
    if dtstart_local and dtend_local:
        lines += [f"DTSTART;TZID={tz_name}:{dt_local_ics(dtstart_local, tz_name)}"]
        lines += [f"DTEND;TZID={tz_name}:{dt_local_ics(dtend_local, tz_name)}"]
    elif dtstart_local:
        lines += [f"DTSTART;TZID={tz_name}:{dt_local_ics(dtstart_local, tz_name)}"]
        # minimal fallback duration
        lines += [f"DTEND;TZID={tz_name}:{dt_local_ics(dtstart_local + timedelta(hours=1), tz_name)}"]
    else:
        # if you ever hit this, you must provide an all-day event instead of a timed event
        # but we keep a hard guard to avoid producing invalid VEVENTs.
        raise ValueError("VEVENT missing DTSTART (dtstart_local is None).")

    if location:
        lines += [f"LOCATION:{ics_escape(location)}"]

    if url:
        lines += [f"URL:{ics_escape(url)}"]

    desc = "\n".join(description_lines).strip()
    lines += [f"DESCRIPTION:{ics_escape(desc)}"]

    lines += ["END:VEVENT"]

    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded


def ics_allday_event(uid: str, summary: str, day_local: datetime, description_lines: List[str]) -> List[str]:
    start_date = day_local.strftime("%Y%m%d")
    end_date = (day_local + timedelta(days=1)).strftime("%Y%m%d")
    desc = "\n".join(description_lines).strip()
    lines = [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        f"SUMMARY:{ics_escape(summary)}",
        f"DTSTART;VALUE=DATE:{start_date}",
        f"DTEND;VALUE=DATE:{end_date}",
        f"DESCRIPTION:{ics_escape(desc)}",
        "END:VEVENT",
    ]
    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded


def ics_calendar_header(calname: str) -> List[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//btsh-ics//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(calname)}",
    ]


def ics_calendar_footer() -> List[str]:
    return ["END:VCALENDAR"]


# -----------------------------
# Domain model
# -----------------------------

@dataclass
class Game:
    game_id: str
    start_utc: Optional[datetime]
    end_utc: Optional[datetime]
    status: str

    cancelled: bool
    placeholder: bool

    home_id: Optional[int]
    home_name: str
    away_id: Optional[int]
    away_name: str

    home_score: Optional[int]
    away_score: Optional[int]

    went_ot: bool
    went_so: bool

    location: str
    note: str


@dataclass
class LeagueDay:
    day_local: datetime  # local midnight
    day_type: str        # e.g. "regular", "make_up", "holiday", etc.
    title: str           # label/title from API
    note: str


# -----------------------------
# API parsing
# -----------------------------

def parse_team_obj(team_obj: Any) -> Tuple[Optional[int], str]:
    if team_obj is None:
        return None, ""
    if isinstance(team_obj, dict):
        tid = to_int(pick(team_obj, ["id"]))
        name = str(pick(team_obj, ["name", "short_name", "display_name"], "") or "").strip()
        return tid, name
    if isinstance(team_obj, int):
        return team_obj, ""
    if isinstance(team_obj, str):
        return None, team_obj.strip()
    return None, ""


def parse_game_obj(g: Dict[str, Any], day_date: Optional[date], tz_name: str) -> Game:
    """
    BTSH game-days provides:
      - day_date on the parent object (YYYY-MM-DD)
      - g["start"]/g["end"] often as time-only ("HH:MM:SS")
    We combine day_date+time into localized datetime and convert to UTC.
    """

    def parse_game_dt(raw: Any) -> Optional[datetime]:
        if raw is None:
            return None
        raw_s = str(raw).strip()
        if not raw_s:
            return None

        # Full ISO datetime
        if "T" in raw_s:
            try:
                dtx = datetime.fromisoformat(raw_s)
            except Exception:
                return None
            if dtx.tzinfo is None:
                dtx = dtx.replace(tzinfo=ZoneInfo(tz_name))
            return dtx.astimezone(timezone.utc)

        # Time-only
        if re.fullmatch(r"\d{2}:\d{2}:\d{2}", raw_s):
            if not day_date:
                return None
            try:
                hh, mm, ss = map(int, raw_s.split(":"))
                local = datetime.combine(day_date, time(hh, mm, ss), tzinfo=ZoneInfo(tz_name))
                return local.astimezone(timezone.utc)
            except Exception:
                return None

        # Date-only fallback
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_s):
            try:
                d = datetime.fromisoformat(raw_s).date()
                local = datetime.combine(d, time(0, 0, 0), tzinfo=ZoneInfo(tz_name))
                return local.astimezone(timezone.utc)
            except Exception:
                return None

        return None

    start_raw = pick(g, ["start", "start_time", "datetime", "game_time", "time", "starts_at"])
    end_raw = pick(g, ["end", "end_time", "ends_at"])

    start_dt = parse_game_dt(start_raw)
    end_dt = parse_game_dt(end_raw)

    dur_min = to_int(pick(g, ["duration", "duration_minutes"]))
    if start_dt and (not end_dt or end_dt <= start_dt):
        if dur_min and dur_min > 0:
            end_dt = start_dt + timedelta(minutes=dur_min)
        else:
            end_dt = start_dt + timedelta(hours=1)

    status = str(pick(g, ["status", "state"], "scheduled") or "scheduled")
    cancelled = bool(pick(g, ["cancelled", "is_cancelled"], False)) or status.strip().lower() in ("cancelled", "canceled")

    home = pick(g, ["home_team", "home", "team_home", "team1", "team_1"])
    away = pick(g, ["away_team", "away", "team_away", "team2", "team_2"])
    home_id, home_name = parse_team_obj(home)
    away_id, away_name = parse_team_obj(away)

    home_name = (pick(g, ["home_team_display"]) or home_name or "").strip()
    away_name = (pick(g, ["away_team_display"]) or away_name or "").strip()

    home_score = to_int(pick(g, ["home_team_num_goals", "home_score", "score_home", "team1_score"]))
    away_score = to_int(pick(g, ["away_team_num_goals", "away_score", "score_away", "team2_score"]))

    went_ot = bool(pick(g, ["went_ot", "overtime", "is_overtime"], False))
    went_so = bool(pick(g, ["went_so", "shootout", "is_shootout"], False))

    location = str(pick(g, ["location", "court", "venue", "rink"], "") or "").strip()

    placeholder = any(
        (s.strip().lower() in ("tbd", "tba", "-", "") or "tbd" in s.strip().lower())
        for s in [home_name or "", away_name or ""]
    )

    note = str(pick(g, ["note", "notes", "label"], "") or "").strip()
    if cancelled and not note:
        note = "Cancelled"
    if placeholder and not note:
        note = "TBD / Placeholder"

    return Game(
        game_id=str(pick(g, ["id", "game_id"], "") or ""),
        start_utc=start_dt,
        end_utc=end_dt,
        status=status,
        cancelled=cancelled,
        placeholder=placeholder,
        home_id=home_id,
        home_name=home_name,
        away_id=away_id,
        away_name=away_name,
        home_score=home_score,
        away_score=away_score,
        went_ot=went_ot,
        went_so=went_so,
        location=location,
        note=note,
    )


def extract_games_from_game_days(game_days: List[dict], tz_name: str) -> List[Game]:
    games: List[Game] = []
    for d in game_days:
        day_raw = d.get("day") or d.get("date")
        day_date: Optional[date] = None
        if isinstance(day_raw, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_raw.strip()):
            try:
                day_date = datetime.fromisoformat(day_raw.strip()).date()
            except Exception:
                day_date = None

        for g in (d.get("games") or []):
            try:
                games.append(parse_game_obj(g, day_date=day_date, tz_name=tz_name))
            except Exception as e:
                print(f"Skipping malformed game object: {e}")
    return games


def extract_league_days_from_game_days(game_days: List[dict], tz_name: str) -> List[LeagueDay]:
    out: List[LeagueDay] = []
    tz = ZoneInfo(tz_name)
    for d in game_days:
        day_raw = d.get("day") or d.get("date")
        if not (isinstance(day_raw, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_raw.strip())):
            continue
        try:
            day_dt = datetime.fromisoformat(day_raw.strip()).replace(tzinfo=tz)
        except Exception:
            continue

        day_type = str(d.get("type") or "").strip()
        title = str(d.get("title") or d.get("label") or d.get("name") or "").strip()
        note = str(d.get("note") or d.get("notes") or "").strip()

        # Heuristic: non-game days = have no games and a non-empty type/title
        if (d.get("games") is None or len(d.get("games") or []) == 0) and (day_type or title or note):
            out.append(LeagueDay(day_local=day_dt, day_type=day_type, title=title, note=note))

    return out


# -----------------------------
# Formatting helpers
# -----------------------------

def to_local(dt_utc: datetime, tz_name: str) -> datetime:
    return dt_utc.astimezone(ZoneInfo(tz_name))


def ascii_rule(title: str) -> List[str]:
    line = "-" * 40
    return [line, title, line]


def ordinal_day(d: int) -> str:
    if 11 <= (d % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(d % 10, "th")
    return f"{d}{suffix}"


def fmt_short_date(dt_local: datetime) -> str:
    # "Feb 20th"
    return f"{dt_local.strftime('%b')} {ordinal_day(dt_local.day)}"


def fmt_result_tag(game: Game, team_is_home: bool) -> str:
    """
    Distinguish regulation vs OT vs SO if the API flags are present.
    W/L tag includes OT/SO suffix when known.
    """
    if game.home_score is None or game.away_score is None:
        return ""  # no result posted yet

    my_score = game.home_score if team_is_home else game.away_score
    opp_score = game.away_score if team_is_home else game.home_score

    outcome = "W" if my_score > opp_score else "L" if my_score < opp_score else "T"
    suffix = ""
    if outcome != "T":
        if game.went_so:
            suffix = " (SO)"
        elif game.went_ot:
            suffix = " (OT)"

    return f"{outcome}{suffix} {my_score}-{opp_score}"


# -----------------------------
# Calendar building
# -----------------------------

def game_involves_team(game: Game, team_name: str) -> bool:
    tn = team_name.strip().lower()
    return (game.home_name or "").strip().lower() == tn or (game.away_name or "").strip().lower() == tn


def opponent_name(game: Game, team_name: str) -> str:
    tn = team_name.strip().lower()
    if (game.home_name or "").strip().lower() == tn:
        return game.away_name
    return game.home_name


def is_home(game: Game, team_name: str) -> bool:
    return (game.home_name or "").strip().lower() == team_name.strip().lower()


def build_opponent_games_to_date(
    all_games: List[Game],
    opp: str,
    event_start_utc: datetime,
    tz_name: str,
    limit: int,
) -> List[str]:
    opp_l = opp.strip().lower()
    rel = [
        g for g in all_games
        if g.start_utc
        and g.start_utc < event_start_utc
        and ((g.home_name or "").strip().lower() == opp_l or (g.away_name or "").strip().lower() == opp_l)
    ]
    rel.sort(key=lambda g: g.start_utc or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    rel = rel[:limit]
    rel.sort(key=lambda g: g.start_utc or datetime.min.replace(tzinfo=timezone.utc))

    out: List[str] = []
    out.extend(ascii_rule(f"{opp.upper()} GAMES-TO-DATE"))

    if not rel:
        out.append("    (none)")
        return out

    for g in rel:
        dt_loc = to_local(g.start_utc, tz_name)
        opp_is_home = (g.home_name or "").strip().lower() == opp_l
        other = g.away_name if opp_is_home else g.home_name
        vs_at = "vs" if opp_is_home else "@"
        tag = fmt_result_tag(g, team_is_home=opp_is_home)
        if tag:
            out.append(f"    {fmt_short_date(dt_loc)} {vs_at} {other} ({tag})")
        else:
            out.append(f"    {fmt_short_date(dt_loc)} {vs_at} {other}")
    return out


def build_head_to_head(
    all_games: List[Game],
    my_team: str,
    opp: str,
    event_start_utc: datetime,
    tz_name: str,
) -> List[str]:
    my_l = my_team.strip().lower()
    opp_l = opp.strip().lower()

    rel = [
        g for g in all_games
        if g.start_utc
        and g.start_utc < event_start_utc
        and (
            ((g.home_name or "").strip().lower() == my_l and (g.away_name or "").strip().lower() == opp_l)
            or
            ((g.home_name or "").strip().lower() == opp_l and (g.away_name or "").strip().lower() == my_l)
        )
    ]
    rel.sort(key=lambda g: g.start_utc or datetime.min.replace(tzinfo=timezone.utc))

    out: List[str] = []
    out.extend(ascii_rule(f"HEAD-TO-HEAD vs {opp}"))

    if not rel:
        out.append("    (no prior matchups)")
        return out

    for g in rel:
        dt_loc = to_local(g.start_utc, tz_name)
        my_is_home = (g.home_name or "").strip().lower() == my_l
        other = g.away_name if my_is_home else g.home_name
        vs_at = "vs" if my_is_home else "@"
        tag = fmt_result_tag(g, team_is_home=my_is_home)
        if tag:
            out.append(f"    {fmt_short_date(dt_loc)} {vs_at} {other} ({tag})")
        else:
            out.append(f"    {fmt_short_date(dt_loc)} {vs_at} {other}")
    return out


def build_game_description(
    cfg: Config,
    game: Game,
    my_team: str,
    all_games: List[Game],
) -> List[str]:
    tz = cfg.default_timezone
    lines: List[str] = []

    # Header
    lines.extend(ascii_rule("GAME INFO"))

    status = game.status.strip().lower()
    if game.cancelled:
        status = "cancelled"

    if game.start_utc:
        start_local = to_local(game.start_utc, tz)
        lines.append(f"Status: {status}")
        lines.append(f"Start ({tz}): {start_local.strftime('%Y-%m-%d %H:%M %Z')}")
    else:
        lines.append(f"Status: {status}")

    if game.location:
        lines.append(f"Location: {game.location}")

    if game.note:
        lines.append(f"Note: {game.note}")

    if cfg.registration_url:
        lines.append("")
        lines.append(f"Check-in / registration: {cfg.registration_url}")

    # Opponent sections only if this looks like a real matchup
    opp = opponent_name(game, my_team).strip()
    if opp and opp != "-" and not game.placeholder and game.start_utc:
        lines.append("")
        lines.extend(build_head_to_head(all_games, my_team=my_team, opp=opp, event_start_utc=game.start_utc, tz_name=tz))
        lines.append("")
        lines.extend(build_opponent_games_to_date(all_games, opp=opp, event_start_utc=game.start_utc, tz_name=tz, limit=cfg.opponent_recent_limit))

    return lines


def write_calendar(path: str, calname: str, event_lines: List[str]) -> None:
    # Use CRLF for maximum compatibility
    content_lines = []
    content_lines.extend(ics_calendar_header(calname))
    content_lines.extend(event_lines)
    content_lines.extend(ics_calendar_footer())
    content = "\r\n".join(content_lines) + "\r\n"

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)


# -----------------------------
# Season lookup (year -> id)
# -----------------------------

def fetch_json(url: str) -> Any:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def season_id_from_year(year: int) -> Optional[int]:
    # This endpoint may return either a list OR a paginated dict with "results"
    seasons = fetch_json("https://api.btsh.org/api/seasons/?")
    if isinstance(seasons, dict) and "results" in seasons:
        seasons = seasons["results"]

    if not isinstance(seasons, list):
        raise RuntimeError("Unexpected seasons response (expected a list).")

    for s in seasons:
        try:
            if int(s.get("year")) == int(year):
                return int(s.get("id"))
        except Exception:
            continue
    return None


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    cfg_path = os.environ.get("CONFIG_PATH", "config.yml")
    cfg = read_config(cfg_path)

    # Determine season id
    season = cfg.season
    if cfg.year is not None:
        print(f"Looking up BTSH season id for year={cfg.year} ...")
        season = season_id_from_year(int(cfg.year))
        if season is None:
            raise RuntimeError(f"Could not find a season with year={cfg.year}")

    if season is None:
        raise RuntimeError("Config must provide either season (id) or year.")

    url = cfg.api_url.format(season=season)
    payload = fetch_json(url)

    # game_days endpoint may return dict w/ results
    game_days = payload["results"] if isinstance(payload, dict) and "results" in payload else payload
    if not isinstance(game_days, list):
        raise RuntimeError("Unexpected game_days response (expected a list or {results:[...]}).")

    tz_name = cfg.default_timezone

    league_days = extract_league_days_from_game_days(game_days, tz_name=tz_name)
    games = extract_games_from_game_days(game_days, tz_name=tz_name)

    # Collect registered teams: prefer the explicit registrations endpoint (season id)
    reg_url = f"https://api.btsh.org/api/team-season-registrations/?season={season}"
    regs = fetch_json(reg_url)
    regs_list = regs["results"] if isinstance(regs, dict) and "results" in regs else regs
    if not isinstance(regs_list, list):
        raise RuntimeError("Unexpected team-season-registrations response.")

    teams: List[Tuple[str, str]] = []  # (team_name, division_name)
    for r in regs_list:
        team = r.get("team") or {}
        div = r.get("division") or {}
        tname = str(team.get("name") or "").strip()
        dname = str(div.get("name") or "").strip()
        if tname:
            teams.append((tname, dname))

    # Also create a season-wide calendar of all games
    year_label = cfg.year if cfg.year else season
    all_events: List[str] = []

    # League-wide days
    if cfg.include_league_wide_days:
        for ld in league_days:
            summary = f"[BTSH] {ld.title or ld.day_type or 'League Day'}"
            desc = []
            desc.extend(ascii_rule("LEAGUE DAY"))
            if ld.day_type:
                desc.append(f"Type: {ld.day_type}")
            if ld.title:
                desc.append(f"Title: {ld.title}")
            if ld.note:
                desc.append(f"Note: {ld.note}")
            uid = f"btsh-league-day-{ld.day_local.strftime('%Y%m%d')}"
            all_events.extend(ics_allday_event(uid, summary, ld.day_local, desc))

    # All games (including placeholders/cancelled) in the all-games calendar
    for g in games:
        if not g.start_utc:
            # skip truly dateless games in all-games calendar
            continue
        start_local = to_local(g.start_utc, tz_name)
        end_local = to_local(g.end_utc, tz_name) if g.end_utc else (start_local + timedelta(hours=1))

        status = g.status.strip().lower()
        if g.cancelled:
            status = "cancelled"

        summary = f"{g.home_name} vs {g.away_name}"
        if status and status != "scheduled":
            summary = f"[{status.upper()}] {summary}"

        desc: List[str] = []
        desc.extend(ascii_rule("GAME INFO"))
        desc.append(f"Status: {status}")
        desc.append(f"Start ({tz_name}): {start_local.strftime('%Y-%m-%d %H:%M %Z')}")
        if g.location:
            desc.append(f"Location: {g.location}")
        if g.note:
            desc.append(f"Note: {g.note}")
        if cfg.registration_url:
            desc.append("")
            desc.append(f"Check-in / registration: {cfg.registration_url}")

        uid = f"btsh-game-{g.game_id}"
        all_events.extend(
            ics_event(
                uid=uid,
                summary=summary,
                dtstart_local=start_local,
                dtend_local=end_local,
                tz_name=tz_name,
                description_lines=desc,
                location=g.location or "",
                url=cfg.registration_url or "",
            )
        )

    all_path = os.path.join(cfg.output_dir, f"btsh-all-games-season-{year_label}.ics")
    write_calendar(all_path, f"BTSH All Games ({year_label})", all_events)
    print(f"Wrote {all_path}")

    # Team calendars
    for team_name, div_name in teams:
        events: List[str] = []

        # Optional: league-wide days on every team calendar
        if cfg.include_league_wide_days:
            for ld in league_days:
                summary = f"[BTSH] {ld.title or ld.day_type or 'League Day'}"
                desc = []
                desc.extend(ascii_rule("LEAGUE DAY"))
                if ld.day_type:
                    desc.append(f"Type: {ld.day_type}")
                if ld.title:
                    desc.append(f"Title: {ld.title}")
                if ld.note:
                    desc.append(f"Note: {ld.note}")
                uid = f"btsh-league-day-{team_name.lower().replace(' ', '-')}-{ld.day_local.strftime('%Y%m%d')}"
                events.extend(ics_allday_event(uid, summary, ld.day_local, desc))

        # Team games
        team_games = [g for g in games if game_involves_team(g, team_name)]

        # If configured, include TBD placeholder games on all calendars
        if cfg.include_tbd_games_on_all_calendars:
            for g in games:
                if g.placeholder and g not in team_games:
                    team_games.append(g)

        # Sort by time
        team_games.sort(key=lambda g: g.start_utc or datetime.max.replace(tzinfo=timezone.utc))

        for g in team_games:
            # Must have a start time to generate a timed VEVENT
            # If missing, skip (or you could convert to all-day if you also have the day date)
            if not g.start_utc:
                continue

            start_local = to_local(g.start_utc, tz_name)
            end_local = to_local(g.end_utc, tz_name) if g.end_utc else (start_local + timedelta(hours=1))

            status = g.status.strip().lower()
            if g.cancelled:
                status = "cancelled"

            opp = opponent_name(g, team_name).strip()
            summary = f"{team_name} vs {opp}" if is_home(g, team_name) else f"{team_name} @ {opp}"

            # Add division in event name (optional feature #3)
            if div_name:
                summary = f"[{div_name}] {summary}"

            if status and status != "scheduled":
                summary = f"[{status.upper()}] {summary}"

            desc = build_game_description(cfg, g, my_team=team_name, all_games=games)
            uid = f"btsh-team-{team_name.lower().replace(' ', '-')}-game-{g.game_id}"

            events.extend(
                ics_event(
                    uid=uid,
                    summary=summary,
                    dtstart_local=start_local,
                    dtend_local=end_local,
                    tz_name=tz_name,
                    description_lines=desc,
                    location=g.location or "",
                    url=cfg.registration_url or "",
                )
            )

        slug = re.sub(r"[^a-z0-9]+", "-", team_name.strip().lower()).strip("-")
        out_path = os.path.join(cfg.output_dir, f"btsh-{slug}-season-{year_label}.ics")
        write_calendar(out_path, f"BTSH {team_name} ({year_label})", events)
        print(f"Wrote {out_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise