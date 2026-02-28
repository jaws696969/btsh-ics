#!/usr/bin/env python3
"""
BTSH ICS generator

Flow:
- Read config.yml (season_year)
- Fetch seasons -> find season_id for that year
- Fetch team-season-registrations for that season_id (source of team list + divisions)
- Fetch game_days for that season_id (SOURCE OF TRUTH for schedule/results)
- Produce:
  - One ICS per registered team (plus placeholders optionally)
  - One master ICS (all games)
- Each event description includes:
  - GAME INFO
  - HEAD-TO-HEAD vs opponent (prior matchups only)
  - OPPONENT GAMES-TO-DATE (prior games only; record computed from completed games w/ scores)
- All times converted/treated as America/New_York
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


# -----------------------------
# Config / Models
# -----------------------------

@dataclass(frozen=True)
class TeamInfo:
    team_id: int
    name: str
    division_name: str
    division_short: str


@dataclass(frozen=True)
class GameRef:
    """Normalized per-game record derived from a game_days 'game' object + its parent day record."""
    game_id: int
    season_year: int
    season_id: int

    day_id: int
    day_type: str
    day_type_display: str
    day_date: date
    day_description: str

    location: str
    court: str

    # game fields
    status: str  # scheduled / completed / cancelled (per your note)
    start_local: Optional[datetime]
    end_local: Optional[datetime]

    home_team_id: Optional[int]
    home_team_name: str
    away_team_id: Optional[int]
    away_team_name: str

    home_score: Optional[int]
    away_score: Optional[int]
    result: Optional[str]  # final / final_ot / final_so / etc

    # opening/closing responsibilities are on the DAY object in the API you uploaded
    opening_team_id: Optional[int]
    closing_team_id: Optional[int]

    # placeholder?
    is_placeholder: bool


# -----------------------------
# HTTP helpers
# -----------------------------

def fetch_json(url: str, timeout: int = 30) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Time helpers
# -----------------------------

def parse_day_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def parse_hh_mm_ss(s: Optional[str]) -> Optional[time]:
    if not s:
        return None
    # game_days uses "15:45:00" strings
    return datetime.strptime(s, "%H:%M:%S").time()

def local_dt(day: date, t: Optional[time], tz: ZoneInfo) -> Optional[datetime]:
    if t is None:
        return None
    return datetime(day.year, day.month, day.day, t.hour, t.minute, t.second, tzinfo=tz)

def ensure_end_after_start(start: Optional[datetime], end: Optional[datetime]) -> Optional[datetime]:
    if start and end and end <= start:
        return end + timedelta(days=1)
    return end

def format_local_dt(dt: Optional[datetime], tz_name: str) -> str:
    if not dt:
        return ""
    # Example: 2025-10-26 15:45 EDT
    return dt.strftime("%Y-%m-%d %H:%M ") + dt.tzname()

def month_day_ordinal(d: date) -> str:
    # Example: Aug 3rd
    suffix = "th"
    if 11 <= d.day <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(d.day % 10, "th")
    return d.strftime("%b ") + f"{d.day}{suffix}"


# -----------------------------
# ICS helpers (RFC 5545-ish)
# -----------------------------

def ics_escape(text: str) -> str:
    # Escape \, ; , , and newlines.
    text = text.replace("\\", "\\\\")
    text = text.replace(";", r"\;").replace(",", r"\,")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", r"\n")
    return text

def fold_ics_line(line: str, limit: int = 75) -> List[str]:
    """
    Fold to 75 octets; we approximate with UTF-8 bytes slicing.
    Continuation lines start with a single space.
    """
    b = line.encode("utf-8")
    if len(b) <= limit:
        return [line]

    out: List[str] = []
    start = 0
    first = True
    while start < len(b):
        chunk = b[start : start + limit]
        # decode chunk safely by backing off if we split a multibyte char
        while True:
            try:
                s = chunk.decode("utf-8")
                break
            except UnicodeDecodeError:
                chunk = chunk[:-1]
        if first:
            out.append(s)
            first = False
        else:
            out.append(" " + s)
        start += len(chunk)
        limit = 74  # continuation lines include leading space, so 74 bytes payload
    return out

def vtimezone_america_new_york() -> List[str]:
    """
    Minimal VTIMEZONE for America/New_York (works for most clients).
    """
    return [
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

def dt_local_ics(dt: datetime) -> str:
    # Floating local with TZID: YYYYMMDDTHHMMSS
    return dt.strftime("%Y%m%dT%H%M%S")

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
    lines.append(f"UID:{uid}")
    lines.append(f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    lines.append(f"SUMMARY:{ics_escape(summary)}")

    if dtstart_local and dtend_local:
        lines.append(f"DTSTART;TZID={tz_name}:{dt_local_ics(dtstart_local)}")
        lines.append(f"DTEND;TZID={tz_name}:{dt_local_ics(dtend_local)}")

    if location:
        lines.append(f"LOCATION:{ics_escape(location)}")

    if url:
        lines.append(f"URL:{ics_escape(url)}")

    desc = "\n".join(description_lines).strip()
    lines.append(f"DESCRIPTION:{ics_escape(desc)}")

    lines.append("END:VEVENT")

    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded

def ics_allday_event(uid: str, summary: str, day_local: date, description_lines: List[str]) -> List[str]:
    # DTSTART;VALUE=DATE:YYYYMMDD
    # DTEND;VALUE=DATE:YYYYMMDD (next day)
    start_date = day_local.strftime("%Y%m%d")
    end_date = (day_local + timedelta(days=1)).strftime("%Y%m%d")
    lines = [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        f"SUMMARY:{ics_escape(summary)}",
        f"DTSTART;VALUE=DATE:{start_date}",
        f"DTEND;VALUE=DATE:{end_date}",
        f"DESCRIPTION:{ics_escape('\\n'.join(description_lines).strip())}",
        "END:VEVENT",
    ]
    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded

def ics_calendar(calname: str, events_lines: List[str], tz_name: str) -> str:
    lines: List[str] = []
    lines.append("BEGIN:VCALENDAR")
    lines.append("VERSION:2.0")
    lines.append("PRODID:-//btsh-ics//EN")
    lines.append("CALSCALE:GREGORIAN")
    lines.append("METHOD:PUBLISH")
    lines.append(f"X-WR-CALNAME:{ics_escape(calname)}")
    lines.append(f"X-WR-TIMEZONE:{tz_name}")
    lines.extend(vtimezone_america_new_york())

    lines.extend(events_lines)
    lines.append("END:VCALENDAR")

    # Use CRLF per spec
    return "\r\n".join(lines) + "\r\n"


# -----------------------------
# Formatting helpers for description blocks
# -----------------------------

def ascii_rule(title: str) -> List[str]:
    bar = "-" * 40
    return [bar, title, bar]

def title_case(s: str) -> str:
    # Used only for headings like "TEAM GAMES-TO-DATE"
    return s.upper()

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "team"

def stable_uid(*parts: str) -> str:
    raw = "|".join(parts).encode("utf-8")
    h = hashlib.sha1(raw).hexdigest()
    return f"{h}@btsh-ics"

def pick_division(team: TeamInfo, fmt: str) -> str:
    return team.division_short if fmt == "short" else team.division_name


# -----------------------------
# Game logic
# -----------------------------

def is_completed_game(g: GameRef) -> bool:
    return (g.status or "").lower() == "completed" and g.away_score is not None and g.home_score is not None

def is_cancelled_game(g: GameRef) -> bool:
    return (g.status or "").lower() == "cancelled"

def game_has_known_teams(g: GameRef) -> bool:
    return g.away_team_name not in ("-", "TBD", "") and g.home_team_name not in ("-", "TBD", "")

def opponent_of(team_id: int, g: GameRef) -> Optional[Tuple[int, str]]:
    if g.home_team_id == team_id and g.away_team_id is not None:
        return (g.away_team_id, g.away_team_name)
    if g.away_team_id == team_id and g.home_team_id is not None:
        return (g.home_team_id, g.home_team_name)
    return None

def team_is_home(team_id: int, g: GameRef) -> bool:
    return g.home_team_id == team_id

def team_is_away(team_id: int, g: GameRef) -> bool:
    return g.away_team_id == team_id

def is_team_in_game(team_id: int, g: GameRef) -> bool:
    return g.home_team_id == team_id or g.away_team_id == team_id

def compare_scores_for_team(team_id: int, g: GameRef) -> Optional[str]:
    """
    Returns 'W'/'L' for a completed game from the perspective of team_id.
    """
    if not is_completed_game(g):
        return None
    assert g.away_score is not None and g.home_score is not None
    if g.home_team_id == team_id:
        return "W" if g.home_score > g.away_score else "L"
    if g.away_team_id == team_id:
        return "W" if g.away_score > g.home_score else "L"
    return None

def result_suffix(g: GameRef) -> str:
    # For description lines: add OT/SO markers when completed
    if not g.result:
        return ""
    r = g.result.lower()
    if "final_ot" in r:
        return " OT"
    if "final_so" in r:
        return " SO"
    return ""

def score_away_home(g: GameRef) -> Optional[str]:
    if g.away_score is None or g.home_score is None:
        return None
    return f"{g.away_score}-{g.home_score}"

def format_game_line_for_team(team_id: int, g: GameRef, opponent_name_override: Optional[str] = None) -> str:
    """
    For 'games-to-date' lists.
    Examples:
      Aug 3rd @ Poutine Machine (L 2-11)
      Feb 6th @ Blizzard
      Oct 12th vs Moby Dekes (Cancelled)
    """
    d = g.day_date
    md = month_day_ordinal(d)

    # Determine opponent / home-away marker from team perspective
    is_home = team_is_home(team_id, g)
    opp_name = opponent_name_override
    if opp_name is None:
        opp = opponent_of(team_id, g)
        opp_name = opp[1] if opp else ("-" if not game_has_known_teams(g) else "?")

    marker = "vs" if is_home else "@"

    # Cancelled
    if is_cancelled_game(g):
        return f"    {md} {marker} {opp_name} (Cancelled)"

    # Completed with score => include W/L + score (+ OT/SO)
    if is_completed_game(g):
        wl = compare_scores_for_team(team_id, g) or ""
        sc = score_away_home(g) or ""
        suf = result_suffix(g)
        return f"    {md} {marker} {opp_name} ({wl} {sc}{suf})"

    # Scheduled / unknown result
    return f"    {md} {marker} {opp_name}"

def compute_record_to_date(team_id: int, games: List[GameRef], before_dt: datetime) -> Tuple[int, int, int, int]:
    """
    Returns (wins, losses, ot_wins, so_wins) for completed games before before_dt.
    Also returns losses total; OT/SO breakdown is on WINS only here for display.
    """
    w = l = otw = sow = 0
    for g in games:
        if not is_team_in_game(team_id, g):
            continue
        if not g.start_local or g.start_local >= before_dt:
            continue
        if not is_completed_game(g):
            continue
        wl = compare_scores_for_team(team_id, g)
        if wl == "W":
            w += 1
            if (g.result or "").lower() == "final_ot":
                otw += 1
            elif (g.result or "").lower() == "final_so":
                sow += 1
        elif wl == "L":
            l += 1
    return w, l, otw, sow


# -----------------------------
# Parsing BTSH payloads
# -----------------------------

def season_id_for_year(seasons_payload: Dict[str, Any], year: int) -> int:
    for s in seasons_payload.get("results", []):
        if int(s.get("year")) == int(year):
            return int(s["id"])
    raise RuntimeError(f"Could not find season with year={year} in seasons API response.")

def parse_team_infos(team_regs_payload: Dict[str, Any]) -> Dict[int, TeamInfo]:
    teams: Dict[int, TeamInfo] = {}
    for r in team_regs_payload.get("results", []):
        t = r.get("team") or {}
        d = r.get("division") or {}
        tid = int(t["id"])
        teams[tid] = TeamInfo(
            team_id=tid,
            name=str(t.get("name") or "").strip(),
            division_name=str(d.get("name") or "").strip(),
            division_short=str(d.get("short_name") or "").strip(),
        )
    return teams

def normalize_game_days(
    game_days_payload: Dict[str, Any],
    season_year: int,
    season_id: int,
    tz: ZoneInfo,
) -> Tuple[List[GameRef], List[Dict[str, Any]]]:
    """
    Returns (games, non_game_days) where:
      - games: list of GameRef for all games
      - non_game_days: raw day objects for non-game day types (scrimmage/holiday/other/etc.)
    """
    games: List[GameRef] = []
    non_game_days: List[Dict[str, Any]] = []

    for day_obj in game_days_payload.get("results", []):
        day_id = int(day_obj["id"])
        day_type = str(day_obj.get("type") or "").strip()
        day_type_display = str(day_obj.get("get_type_display") or day_type).strip()
        day_date = parse_day_yyyy_mm_dd(str(day_obj.get("day")))
        day_desc = (day_obj.get("description") or "").strip()

        location = (day_obj.get("location") or "").strip()
        court = (day_obj.get("court") or "").strip()

        opening_team = day_obj.get("opening_team")
        closing_team = day_obj.get("closing_team")
        opening_team_id = int(opening_team["id"]) if isinstance(opening_team, dict) and opening_team.get("id") else None
        closing_team_id = int(closing_team["id"]) if isinstance(closing_team, dict) and closing_team.get("id") else None

        # Non-game day records (no games list, or day_type != "game")
        if day_type != "game" and not day_obj.get("games"):
            non_game_days.append(day_obj)
            continue

        for g in day_obj.get("games", []) or []:
            game_id = int(g["id"])
            status = str(g.get("status") or "").strip()

            # times are "HH:MM:SS"
            start_t = parse_hh_mm_ss(g.get("start"))
            end_t = parse_hh_mm_ss(g.get("end"))
            start_local = local_dt(day_date, start_t, tz)
            end_local = ensure_end_after_start(start_local, local_dt(day_date, end_t, tz))

            home_team = g.get("home_team")
            away_team = g.get("away_team")
            home_team_id = int(home_team["id"]) if isinstance(home_team, dict) and home_team.get("id") else None
            away_team_id = int(away_team["id"]) if isinstance(away_team, dict) and away_team.get("id") else None
            home_team_name = (home_team.get("name") if isinstance(home_team, dict) else None) or "-"
            away_team_name = (away_team.get("name") if isinstance(away_team, dict) else None) or "-"

            home_score = g.get("home_score")
            away_score = g.get("away_score")
            home_score = int(home_score) if home_score is not None else None
            away_score = int(away_score) if away_score is not None else None

            result = g.get("result")

            # Some payloads might include location/court at game level; fall back to day
            gloc = (g.get("location") or location).strip()
            gcourt = (g.get("court") or court).strip()

            is_placeholder = (home_team_name in ("-", "TBD", "")) or (away_team_name in ("-", "TBD", ""))

            games.append(
                GameRef(
                    game_id=game_id,
                    season_year=season_year,
                    season_id=season_id,
                    day_id=day_id,
                    day_type=day_type,
                    day_type_display=day_type_display,
                    day_date=day_date,
                    day_description=day_desc,
                    location=gloc,
                    court=gcourt,
                    status=status,
                    start_local=start_local,
                    end_local=end_local,
                    home_team_id=home_team_id,
                    home_team_name=str(home_team_name).strip(),
                    away_team_id=away_team_id,
                    away_team_name=str(away_team_name).strip(),
                    home_score=home_score,
                    away_score=away_score,
                    result=str(result).strip() if result is not None else None,
                    opening_team_id=opening_team_id,
                    closing_team_id=closing_team_id,
                    is_placeholder=is_placeholder,
                )
            )

    # Sort by start time if available, else by date
    games.sort(key=lambda x: (x.start_local or datetime(x.day_date.year, x.day_date.month, x.day_date.day, tzinfo=tz), x.game_id))
    return games, non_game_days


# -----------------------------
# Event building
# -----------------------------

def build_summary_for_team_calendar(
    team: TeamInfo,
    g: GameRef,
    cfg: Dict[str, Any],
    team_map: Dict[int, TeamInfo],
) -> str:
    tz_name = cfg["default_timezone"]
    cancelled_prefix = cfg.get("cancelled_prefix", "CANCELLED:")
    include_div = bool(cfg.get("include_division_in_summary", False))
    div_fmt = str(cfg.get("division_format", "short"))

    # Determine opponent + vs/@ from the calendar team's perspective
    if g.home_team_id == team.team_id:
        opp_name = g.away_team_name
        marker = "vs"
    else:
        opp_name = g.home_team_name
        marker = "@"

    # Base: "[Opening] [Closing] TEAM vs/@ OPP"
    tags: List[str] = []
    if g.opening_team_id == team.team_id:
        tags.append("[Opening]")
    if g.closing_team_id == team.team_id:
        tags.append("[Closing]")

    team_label = team.name
    if include_div:
        team_label = f"{team_label} ({pick_division(team, div_fmt)})"

    # If opponent is registered, optionally annotate division too
    opp_label = opp_name
    if include_div:
        opp_info = team_map.get(g.away_team_id if g.home_team_id == team.team_id else g.home_team_id)
        if opp_info:
            opp_label = f"{opp_label} ({pick_division(opp_info, div_fmt)})"

    summary = " ".join(tags + [f"{team_label} {marker} {opp_label}"]).strip()

    # If completed, append score (away-home) and OT/SO suffix; DO NOT add W/L to summary
    if is_completed_game(g):
        sc = score_away_home(g)
        if sc:
            suf = ""
            if (g.result or "").lower() == "final_ot":
                suf = " (OT)"
            elif (g.result or "").lower() == "final_so":
                suf = " (SO)"
            summary = f"{summary} {sc}{suf}"

    # If cancelled, prefix
    if is_cancelled_game(g):
        summary = f"{cancelled_prefix} {summary}"

    return summary

def build_summary_for_master_calendar(g: GameRef, cfg: Dict[str, Any]) -> str:
    cancelled_prefix = cfg.get("cancelled_prefix", "CANCELLED:")

    # Master summary: Away @ Home
    summary = f"{g.away_team_name} @ {g.home_team_name}".strip()

    if is_completed_game(g):
        sc = score_away_home(g)
        if sc:
            suf = ""
            if (g.result or "").lower() == "final_ot":
                suf = " (OT)"
            elif (g.result or "").lower() == "final_so":
                suf = " (SO)"
            summary = f"{summary} {sc}{suf}"

    if is_cancelled_game(g):
        summary = f"{cancelled_prefix} {summary}"

    return summary

def build_description_for_team_event(
    calendar_team: TeamInfo,
    opponent_name: str,
    g: GameRef,
    all_games: List[GameRef],
    cfg: Dict[str, Any],
    tz_name: str,
) -> List[str]:
    checkin_label = cfg.get("checkin_label", "Check-in / Standings")
    checkin_url = cfg.get("checkin_url", "https://btsh.org")
    opponent_games_limit = cfg.get("opponent_games_limit", None)
    if isinstance(opponent_games_limit, str) and opponent_games_limit.lower() == "null":
        opponent_games_limit = None

    desc: List[str] = []

    # GAME INFO
    desc.extend(ascii_rule("GAME INFO"))
    desc.append(f"Season: {calendar_team and g.season_year}")
    desc.append(f"Stage: {g.day_type_display}")
    desc.append(f"Status: {g.status}")
    desc.append(f"Start ({tz_name}): {format_local_dt(g.start_local, tz_name)}")
    # Location: "Tompkins Square Park (West)" (from your example)
    loc_parts = [p for p in [g.location, g.court] if p]
    rink = " - ".join(loc_parts) if loc_parts else ""
    if g.location and g.court:
        rink = f"{g.location} ({g.court})"
    elif g.location:
        rink = g.location
    elif g.court:
        rink = g.court
    if rink:
        desc.append(f"Rink: {rink}")
    desc.append(f"{checkin_label}: {checkin_url}")
    desc.append("")

    # HEAD-TO-HEAD
    desc.extend(ascii_rule(f"HEAD-TO-HEAD vs {opponent_name}"))
    prior_h2h = []
    if g.start_local:
        for gg in all_games:
            if not gg.start_local or gg.start_local >= g.start_local:
                continue
            # both teams involved
            if is_team_in_game(calendar_team.team_id, gg) and (
                (gg.home_team_name == opponent_name) or (gg.away_team_name == opponent_name)
            ):
                prior_h2h.append(gg)

    if not prior_h2h:
        desc.append("    (no prior matchups listed)")
    else:
        for gg in prior_h2h:
            # format from calendar_team perspective, but opponent name already known
            desc.append(format_game_line_for_team(calendar_team.team_id, gg, opponent_name_override=opponent_name).rstrip())

    desc.append("")

    # OPPONENT GAMES-TO-DATE
    desc.extend(ascii_rule(f"{title_case(opponent_name)} GAMES-TO-DATE"))

    # Determine opponent team id from the actual event game (if available)
    opp_id = None
    if g.home_team_id == calendar_team.team_id:
        opp_id = g.away_team_id
    elif g.away_team_id == calendar_team.team_id:
        opp_id = g.home_team_id

    # Build list of opponent prior games (prior to event start; within season)
    opp_prior: List[GameRef] = []
    if g.start_local and opp_id is not None:
        for gg in all_games:
            if not gg.start_local or gg.start_local >= g.start_local:
                continue
            if is_team_in_game(opp_id, gg):
                opp_prior.append(gg)

    # Record to date (completed games only) BEFORE event
    if g.start_local and opp_id is not None:
        w, l, otw, sow = compute_record_to_date(opp_id, all_games, g.start_local)
        record_str = f"{w}-{l}"
        # Include OT/SO win breakdown since you asked to distinguish these
        extra = []
        if otw:
            extra.append(f"OT W: {otw}")
        if sow:
            extra.append(f"SO W: {sow}")
        extra_str = f" ({', '.join(extra)})" if extra else ""
        desc.append(f"Record to date (completed games only): {record_str}{extra_str}")

    if opponent_games_limit is not None:
        try:
            opponent_games_limit = int(opponent_games_limit)
        except Exception:
            opponent_games_limit = None

    # If limit set, keep most recent prior games (still before event start)
    if opponent_games_limit is not None and opponent_games_limit > 0:
        opp_prior = opp_prior[-opponent_games_limit:]

    if not opp_prior:
        desc.append("    (no prior games listed)")
    else:
        for gg in opp_prior:
            # Format from opponent perspective so W/L makes sense for them
            desc.append(format_game_line_for_team(opp_id, gg).rstrip())

    return desc


# -----------------------------
# Main generation
# -----------------------------

def load_config(path: str = "config.yml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    # Basic required keys
    if "season_year" not in cfg:
        raise RuntimeError("config.yml missing required key: season_year")
    if "output_dir" not in cfg:
        cfg["output_dir"] = "docs"
    if "default_timezone" not in cfg:
        cfg["default_timezone"] = "America/New_York"
    return cfg

def write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)

def main() -> None:
    cfg = load_config("config.yml")

    tz_name = str(cfg["default_timezone"])
    tz = ZoneInfo(tz_name)

    season_year = int(cfg["season_year"])
    seasons_api_url = str(cfg["seasons_api_url"])
    team_regs_url_tmpl = str(cfg["team_registrations_api_url"])
    game_days_url_tmpl = str(cfg["game_days_api_url"])

    include_placeholders = bool(cfg.get("include_placeholders", True))
    include_cancelled_games = bool(cfg.get("include_cancelled_games", True))

    team_day_types = set([str(x).strip() for x in (cfg.get("team_calendar_day_types") or [])])
    master_day_types = set([str(x).strip() for x in (cfg.get("master_calendar_day_types") or [])])

    include_non_game_days = bool(cfg.get("include_non_game_days_as_all_day_events", True))

    out_dir = str(cfg["output_dir"])
    team_file_prefix = str(cfg.get("team_file_prefix", "btsh"))
    master_name_tmpl = str(cfg.get("master_file_name_template", "btsh-all-games-season-{year}.ics"))

    # 1) Seasons: find season id by year
    seasons_payload = fetch_json(seasons_api_url)
    season_id = season_id_for_year(seasons_payload, season_year)

    # 2) Team registrations (registered teams + divisions)
    team_regs_payload = fetch_json(team_regs_url_tmpl.format(season_id=season_id))
    team_map = parse_team_infos(team_regs_payload)

    # 3) Game days (source of truth)
    game_days_payload = fetch_json(game_days_url_tmpl.format(season_id=season_id))
    games, non_game_days = normalize_game_days(game_days_payload, season_year, season_id, tz)

    # Helper: filter games for a given calendar's allowed day types
    def calendar_game_filter(g: GameRef, allowed_day_types: set) -> bool:
        if g.day_type not in allowed_day_types:
            return False
        if g.is_placeholder and not include_placeholders:
            return False
        if is_cancelled_game(g) and not include_cancelled_games:
            return False
        return True

    # Build master calendar events
    master_events: List[str] = []
    for g in games:
        if not calendar_game_filter(g, master_day_types):
            continue
        if not g.start_local or not g.end_local:
            # If missing times, skip (shouldn't happen for games)
            continue

        summary = build_summary_for_master_calendar(g, cfg)

        location = ""
        if g.location and g.court:
            location = f"{g.location} ({g.court})"
        elif g.location:
            location = g.location

        desc_lines: List[str] = []
        desc_lines.extend(ascii_rule("GAME INFO"))
        desc_lines.append(f"Season: {season_year}")
        desc_lines.append(f"Stage: {g.day_type_display}")
        desc_lines.append(f"Status: {g.status}")
        desc_lines.append(f"Start ({tz_name}): {format_local_dt(g.start_local, tz_name)}")
        if location:
            desc_lines.append(f"Rink: {location}")
        desc_lines.append(f"{cfg.get('checkin_label','Check-in / Standings')}: {cfg.get('checkin_url','https://btsh.org')}")

        uid = stable_uid("master", str(season_id), str(g.game_id))
        ev_lines = ics_event(
            uid=uid,
            summary=summary,
            dtstart_local=g.start_local,
            dtend_local=g.end_local,
            tz_name=tz_name,
            description_lines=desc_lines,
            location=location,
            url=str(cfg.get("checkin_url", "")),
        )
        master_events.extend(ev_lines)

    # Include non-game days as all-day events (master)
    if include_non_game_days:
        for d in non_game_days:
            day_type = str(d.get("type") or "").strip()
            if day_type not in master_day_types:
                continue
            day_date = parse_day_yyyy_mm_dd(str(d.get("day")))
            title = str(d.get("get_type_display") or day_type).strip()
            desc = (d.get("description") or "").strip()
            summary = f"{title}"
            uid = stable_uid("master-day", str(season_id), str(d.get("id")))
            master_events.extend(
                ics_allday_event(uid=uid, summary=summary, day_local=day_date, description_lines=[desc] if desc else [])
            )

    master_calname = f"BTSH All Games ({season_year})"
    master_content = ics_calendar(master_calname, master_events, tz_name)
    master_path = os.path.join(out_dir, master_name_tmpl.format(year=season_year))
    write_text(master_path, master_content)

    # Build each team calendar
    for team_id, team in sorted(team_map.items(), key=lambda kv: kv[1].name.lower()):
        team_events: List[str] = []

        # Team games
        for g in games:
            # include games where team is participating OR (placeholder inclusion logic)
            if not is_team_in_game(team_id, g):
                continue
            if not calendar_game_filter(g, team_day_types):
                continue
            if not g.start_local or not g.end_local:
                continue

            # Determine opponent name (even if placeholder)
            if g.home_team_id == team_id:
                opp_name = g.away_team_name
            else:
                opp_name = g.home_team_name

            summary = build_summary_for_team_calendar(team, g, cfg, team_map)

            location = ""
            if g.location and g.court:
                location = f"{g.location} ({g.court})"
            elif g.location:
                location = g.location

            desc_lines = build_description_for_team_event(
                calendar_team=team,
                opponent_name=opp_name,
                g=g,
                all_games=games,
                cfg=cfg,
                tz_name=tz_name,
            )

            uid = stable_uid("team", str(team_id), str(season_id), str(g.game_id))
            ev_lines = ics_event(
                uid=uid,
                summary=summary,
                dtstart_local=g.start_local,
                dtend_local=g.end_local,
                tz_name=tz_name,
                description_lines=desc_lines,
                location=location,
                url=str(cfg.get("checkin_url", "")),
            )
            team_events.extend(ev_lines)

        # Team non-game days (all-day), only if configured day types include them
        if include_non_game_days:
            for d in non_game_days:
                day_type = str(d.get("type") or "").strip()
                if day_type not in team_day_types:
                    continue
                day_date = parse_day_yyyy_mm_dd(str(d.get("day")))
                title = str(d.get("get_type_display") or day_type).strip()
                desc = (d.get("description") or "").strip()
                summary = f"{title}"
                uid = stable_uid("team-day", str(team_id), str(season_id), str(d.get("id")))
                team_events.extend(
                    ics_allday_event(uid=uid, summary=summary, day_local=day_date, description_lines=[desc] if desc else [])
                )

        calname = f"BTSH {team.name} ({season_year})"
        content = ics_calendar(calname, team_events, tz_name)

        filename = f"{team_file_prefix}-{slugify(team.name)}-season-{season_year}.ics"
        path = os.path.join(out_dir, filename)
        write_text(path, content)

    print(f"Generated {len(team_map)} team calendars + master calendar for season {season_year} (id={season_id})")
    print(f"Output directory: {out_dir}")


if __name__ == "__main__":
    main()