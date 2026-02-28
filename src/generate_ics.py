#!/usr/bin/env python3
"""
BTSH ICS generator

Flow:
- Read config.yml (season_year, output_dir, etc.)
- GET seasons -> find season_id by year
- GET team-season-registrations/?season={season_id} -> registered teams + divisions
- GET game_days/?season={season_id} -> schedule + results (source of truth)
- Build:
  - one calendar per registered team
  - one master league calendar (all games)

Notes:
- game_days endpoint returns paginated response with "results"
- Scores live in: away_team_num_goals / home_team_num_goals
- OT/SO marker lives in: result = final | final_ot | final_so
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Python <3.9 not supported by your workflow anyway
    ZoneInfo = None  # type: ignore


# -----------------------------
# Config + constants
# -----------------------------

DEFAULT_CONFIG_PATH = "config.yml"
PRODID = "-//btsh-ics//EN"
BTSH_HOME_URL = "https://btsh.org"

SEASONS_URL = "https://api.btsh.org/api/seasons/"
TEAM_REGS_URL_TMPL = "https://api.btsh.org/api/team-season-registrations/?season={season_id}"
GAME_DAYS_URL_TMPL = "https://api.btsh.org/api/game_days/?season={season_id}"


# -----------------------------
# Helpers: HTTP + paging
# -----------------------------

def http_get_json(url: str, timeout: int = 30) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_results(payload: Any) -> List[Any]:
    """
    BTSH APIs are typically paginated dicts with "results".
    Sometimes you may get a list directly; handle both.
    """
    if isinstance(payload, dict) and "results" in payload and isinstance(payload["results"], list):
        return payload["results"]
    if isinstance(payload, list):
        return payload
    # Last resort: treat single object as one-item list
    return [payload]


# -----------------------------
# Helpers: ICS formatting
# -----------------------------

def ics_escape(s: str) -> str:
    """
    RFC5545 text escaping:
    - backslash
    - semicolon
    - comma
    - newline -> \n
    """
    s = s.replace("\\", "\\\\")
    s = s.replace(";", "\\;")
    s = s.replace(",", "\\,")
    s = s.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    return s


def fold_ics_line(line: str, limit: int = 75) -> List[str]:
    """
    Fold long lines per RFC: subsequent lines start with a single space.
    This implementation folds by character count (good enough for typical ASCII content).
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


def dt_local_ics(dt_local: datetime) -> str:
    # DTSTART;TZID=America/New_York:YYYYMMDDTHHMMSS
    return dt_local.strftime("%Y%m%dT%H%M%S")


def format_dt_local(dt_local: datetime, tz_name: str) -> str:
    # Human readable: 2025-10-26 15:45 EDT
    # %Z gives EDT/EST if tzinfo is correct.
    return dt_local.strftime("%Y-%m-%d %H:%M %Z")


def ordinal_day(n: int) -> str:
    # 1st 2nd 3rd 4th...
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def fmt_short_date(dt_local: datetime) -> str:
    # "Aug 3rd"
    return dt_local.strftime("%b ") + ordinal_day(dt_local.day)


def ascii_rule(title: str, width: int = 40) -> List[str]:
    bar = "-" * width
    return [bar, title.strip(), bar]


def stable_uid(*parts: str) -> str:
    """
    Stable UID so updates don't duplicate events.
    Use a hash of identifying fields.
    """
    raw = "|".join(parts).encode("utf-8")
    h = hashlib.sha1(raw).hexdigest()
    return f"{h}@btsh-ics"


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

    desc_str = "\n".join(description_lines).strip()
    lines.append(f"DESCRIPTION:{ics_escape(desc_str)}")

    lines.append("END:VEVENT")

    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded


def ics_allday_event(uid: str, summary: str, day_local: datetime, description_lines: List[str]) -> List[str]:
    # DTSTART;VALUE=DATE:YYYYMMDD
    start_date = day_local.strftime("%Y%m%d")
    end_date = (day_local + timedelta(days=1)).strftime("%Y%m%d")
    desc_str = "\n".join(description_lines).strip()

    lines = [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        f"SUMMARY:{ics_escape(summary)}",
        f"DTSTART;VALUE=DATE:{start_date}",
        f"DTEND;VALUE=DATE:{end_date}",
        f"DESCRIPTION:{ics_escape(desc_str)}",
        "END:VEVENT",
    ]
    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded


def calendar_header(cal_name: str, tz_name: str) -> List[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{PRODID}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(cal_name)}",
        f"X-WR-TIMEZONE:{ics_escape(tz_name)}",
    ]


def calendar_footer() -> List[str]:
    return ["END:VCALENDAR"]


# -----------------------------
# Domain model
# -----------------------------

@dataclass(frozen=True)
class Team:
    id: int
    name: str
    division_name: str = ""
    division_short: str = ""


@dataclass
class Game:
    id: int
    type: str  # regular season / playoff / scrimmage / make_up / etc. from display
    status: str  # scheduled / completed / cancelled
    dt_local: Optional[datetime]
    duration_min: int

    home_team_id: Optional[int]
    home_team_name: str
    away_team_id: Optional[int]
    away_team_name: str

    home_goals: Optional[int]
    away_goals: Optional[int]
    result_code: str  # final / final_ot / final_so (or "")

    location: str
    court: str

    opening_team_id: Optional[int]
    closing_team_id: Optional[int]

    raw: Dict[str, Any]


# -----------------------------
# Parsing BTSH game_days payload
# -----------------------------

def parse_dt_to_local(dt_str: Optional[str], tz_name: str) -> Optional[datetime]:
    if not dt_str:
        return None
    # Example: "2025-10-26T15:45:00-04:00"
    try:
        dt = datetime.fromisoformat(dt_str)
    except Exception:
        return None
    if ZoneInfo is None:
        return dt
    return dt.astimezone(ZoneInfo(tz_name))


def normalize_str(s: Any) -> str:
    return str(s).strip() if s is not None else ""


def team_obj_id_name(team_obj: Any) -> Tuple[Optional[int], str]:
    if isinstance(team_obj, dict):
        tid = team_obj.get("id")
        name = team_obj.get("name") or ""
        try:
            tid_int = int(tid) if tid is not None else None
        except Exception:
            tid_int = None
        return tid_int, normalize_str(name)
    # If API gives a string like "-" / "TBD"
    return None, normalize_str(team_obj)


def is_placeholder_team(name: str) -> bool:
    n = name.strip().lower()
    return n in {"", "-", "tbd", "to be determined", "placeholder"}


def parse_games(game_days_payload: Any, tz_name: str, default_duration_min: int = 60) -> List[Game]:
    days = extract_results(game_days_payload)
    games: List[Game] = []

    for day in days:
        if not isinstance(day, dict):
            continue

        opening_id, _opening_name = team_obj_id_name(day.get("opening_team"))
        closing_id, _closing_name = team_obj_id_name(day.get("closing_team"))

        for g in day.get("games") or []:
            if not isinstance(g, dict):
                continue

            gid = int(g.get("id")) if g.get("id") is not None else 0
            status = normalize_str(g.get("status")).lower()  # scheduled/completed/cancelled
            dt_local = parse_dt_to_local(g.get("start_time"), tz_name)

            # Type/stage: prefer display if present
            stage = normalize_str(g.get("get_type_display") or g.get("type") or day.get("get_type_display") or day.get("type"))

            home_id, home_name = team_obj_id_name(g.get("home_team"))
            away_id, away_name = team_obj_id_name(g.get("away_team"))

            home_goals = g.get("home_team_num_goals")
            away_goals = g.get("away_team_num_goals")
            try:
                home_goals = int(home_goals) if home_goals is not None else None
            except Exception:
                home_goals = None
            try:
                away_goals = int(away_goals) if away_goals is not None else None
            except Exception:
                away_goals = None

            result_code = normalize_str(g.get("result")).lower()

            location = normalize_str(g.get("location") or day.get("location"))
            court = normalize_str(g.get("court") or "")

            duration_min = default_duration_min
            if g.get("duration_minutes") is not None:
                try:
                    duration_min = int(g.get("duration_minutes"))
                except Exception:
                    pass

            games.append(
                Game(
                    id=gid,
                    type=stage if stage else "Game",
                    status=status if status else "scheduled",
                    dt_local=dt_local,
                    duration_min=duration_min,
                    home_team_id=home_id,
                    home_team_name=home_name,
                    away_team_id=away_id,
                    away_team_name=away_name,
                    home_goals=home_goals,
                    away_goals=away_goals,
                    result_code=result_code,
                    location=location,
                    court=court,
                    opening_team_id=opening_id,
                    closing_team_id=closing_id,
                    raw=g,
                )
            )

    # sort stable
    games.sort(key=lambda x: (x.dt_local or datetime(1970, 1, 1, tzinfo=timezone.utc), x.id))
    return games


# -----------------------------
# Records, results, formatting
# -----------------------------

def is_completed_with_score(g: Game) -> bool:
    return g.status == "completed" and g.home_goals is not None and g.away_goals is not None


def score_suffix(g: Game) -> str:
    # result_code values: final / final_ot / final_so
    if g.result_code == "final_ot":
        return " (OT)"
    if g.result_code == "final_so":
        return " (SO)"
    return ""


def score_away_home(g: Game) -> Optional[str]:
    if g.away_goals is None or g.home_goals is None:
        return None
    return f"{g.away_goals}-{g.home_goals}{score_suffix(g)}"


def winner_for_team(g: Game, team_id: int) -> Optional[str]:
    """
    Returns "W" / "L" from perspective of team_id for completed games with scores.
    """
    if not is_completed_with_score(g):
        return None
    if team_id not in {g.home_team_id, g.away_team_id}:
        return None

    # Determine team goals vs opponent goals
    if team_id == g.away_team_id:
        my_goals, opp_goals = g.away_goals, g.home_goals
    else:
        my_goals, opp_goals = g.home_goals, g.away_goals

    if my_goals is None or opp_goals is None:
        return None
    return "W" if my_goals > opp_goals else "L"


def format_location(location: str, court: str) -> str:
    if not location and not court:
        return ""
    if location and court:
        return f"{location} ({court.title()})"
    return location or court.title()


def opponent_record_to_date(opponent_id: int, games: List[Game], before_dt: Optional[datetime]) -> Tuple[int, int]:
    """
    Record for opponent_id in completed games only (W-L) up to before_dt (exclusive).
    """
    wins = 0
    losses = 0
    for g in games:
        if before_dt and g.dt_local and g.dt_local >= before_dt:
            continue
        if not is_completed_with_score(g):
            continue
        if opponent_id not in {g.home_team_id, g.away_team_id}:
            continue

        # Determine opponent goals vs other goals
        if opponent_id == g.away_team_id:
            my_goals, opp_goals = g.away_goals, g.home_goals
        else:
            my_goals, opp_goals = g.home_goals, g.away_goals

        if my_goals is None or opp_goals is None:
            continue
        if my_goals > opp_goals:
            wins += 1
        else:
            losses += 1
    return wins, losses


def list_games_for_team_before(
    team_id: int,
    games: List[Game],
    before_dt: Optional[datetime],
) -> List[Game]:
    out: List[Game] = []
    for g in games:
        if team_id not in {g.home_team_id, g.away_team_id}:
            continue
        if before_dt and g.dt_local and g.dt_local >= before_dt:
            continue
        out.append(g)
    out.sort(key=lambda x: (x.dt_local or datetime(1970, 1, 1, tzinfo=timezone.utc), x.id))
    return out


def list_head_to_head_before(
    team_id: int,
    opp_id: int,
    games: List[Game],
    before_dt: Optional[datetime],
) -> List[Game]:
    out: List[Game] = []
    for g in games:
        if {team_id, opp_id} != {g.home_team_id, g.away_team_id}:
            continue
        if before_dt and g.dt_local and g.dt_local >= before_dt:
            continue
        out.append(g)
    out.sort(key=lambda x: (x.dt_local or datetime(1970, 1, 1, tzinfo=timezone.utc), x.id))
    return out


def line_for_game_from_team_pov(g: Game, pov_team_id: int, tz_name: str) -> str:
    """
    Example:
        Aug 3rd @ Poutine Machine (2-11)
        Oct 12th vs Moby Dekes (Cancelled)
        Feb 6th @ Blizzard
    """
    dt = g.dt_local
    date_part = fmt_short_date(dt) if dt else "TBD"

    if pov_team_id == g.home_team_id:
        opp_name = g.away_team_name
        at_vs = "vs"
    else:
        opp_name = g.home_team_name
        at_vs = "@"

    # Cancelled
    if g.status == "cancelled":
        return f"    {date_part} {at_vs} {opp_name} (Cancelled)"

    # Completed w/ score
    sc = score_away_home(g)
    if g.status == "completed" and sc:
        # From POV, we still show the neutral away-home score per your spec
        return f"    {date_part} {at_vs} {opp_name} ({sc})"

    # Scheduled / completed but no score posted yet
    return f"    {date_part} {at_vs} {opp_name}"


# -----------------------------
# Calendars building
# -----------------------------

def calendar_summary_for_team_game(team: Team, g: Game, include_division: bool) -> str:
    """
    Per-team calendar:
      scheduled: "@ Opp" or "vs Opp"
      completed: "@ Opp — 5-4 (SO)" (score is away-home per spec)
      cancelled: "CANCELLED: @ Opp"
      Opening/Closing tags: add "[Opening]" / "[Closing]"
    """
    # Determine opponent and home/away marker
    if team.id == g.home_team_id:
        base = f"vs {g.away_team_name}"
    else:
        base = f"@ {g.home_team_name}"

    tags: List[str] = []
    if g.opening_team_id == team.id:
        tags.append("[Opening]")
    if g.closing_team_id == team.id:
        tags.append("[Closing]")

    prefix = "CANCELLED: " if g.status == "cancelled" else ""
    div = f"[D{team.division_short}] " if (include_division and team.division_short) else ""
    tag_str = (" " + " ".join(tags)) if tags else ""

    if g.status == "completed":
        sc = score_away_home(g)
        if sc:
            return f"{prefix}{div}{base} — {sc}{tag_str}"
        return f"{prefix}{div}{base}{tag_str}"

    return f"{prefix}{div}{base}{tag_str}"


def calendar_summary_for_master_game(g: Game) -> str:
    """
    Master calendar: "Away @ Home" plus score if completed.
    Cancelled prefix always.
    """
    base = f"{g.away_team_name} @ {g.home_team_name}"
    prefix = "CANCELLED: " if g.status == "cancelled" else ""
    if g.status == "completed":
        sc = score_away_home(g)
        if sc:
            return f"{prefix}{base} — {sc}"
    return f"{prefix}{base}"


def build_description(
    season_year: int,
    tz_name: str,
    team: Optional[Team],
    opponent: Optional[Team],
    game: Game,
    all_games: List[Game],
    opponent_recent_limit: int,
) -> List[str]:
    """
    Description per your template.
    """
    desc: List[str] = []
    desc.extend(ascii_rule("GAME INFO"))
    desc.append(f"Season: {season_year}")
    desc.append(f"Stage: {game.type}")
    desc.append(f"Status: {game.status}")

    if game.dt_local:
        desc.append(f"Start ({tz_name}): {format_dt_local(game.dt_local, tz_name)}")

    loc = format_location(game.location, game.court)
    if loc:
        desc.append(f"Rink: {loc}")

    desc.append(f"Check-in / Standings: {BTSH_HOME_URL}")
    desc.append("")

    # Team-specific sections
    if team and opponent:
        # Head-to-head
        desc.extend(ascii_rule(f"HEAD-TO-HEAD vs {opponent.name}"))
        h2h = list_head_to_head_before(team.id, opponent.id, all_games, game.dt_local)
        if not h2h:
            desc.append("    (no prior matchups listed)")
        else:
            for g in h2h:
                # Show as: "Feb 20th vs Slainte Ice Dragons" (no team repetition)
                dt = g.dt_local
                date_part = fmt_short_date(dt) if dt else "TBD"

                # From TEAM POV: was it vs or @
                if team.id == g.home_team_id:
                    at_vs = "vs"
                else:
                    at_vs = "@"

                if g.status == "cancelled":
                    desc.append(f"    {date_part} {at_vs} {opponent.name} (Cancelled)")
                elif g.status == "completed":
                    sc = score_away_home(g)
                    if sc:
                        desc.append(f"    {date_part} {at_vs} {opponent.name} ({sc})")
                    else:
                        desc.append(f"    {date_part} {at_vs} {opponent.name}")
                else:
                    desc.append(f"    {date_part} {at_vs} {opponent.name}")

        desc.append("")

        # Opponent games-to-date
        desc.extend(ascii_rule(f"{opponent.name.upper()} GAMES-TO-DATE"))

        # Record to date (completed games only)
        ow, ol = opponent_record_to_date(opponent.id, all_games, game.dt_local)
        desc.append(f"Record to date (completed games only): {ow}-{ol}")

        opp_games = list_games_for_team_before(opponent.id, all_games, game.dt_local)
        # Apply limit from most recent backwards, but keep chronological display
        if opponent_recent_limit and len(opp_games) > opponent_recent_limit:
            opp_games = opp_games[-opponent_recent_limit:]

        if not opp_games:
            desc.append("    (no prior games listed)")
        else:
            for g in opp_games:
                desc.append(line_for_game_from_team_pov(g, opponent.id, tz_name))

    return desc


def should_include_game_for_team_calendar(
    g: Game,
    team_id: int,
    include_day_types: List[str],
    include_placeholders: bool,
) -> bool:
    if team_id not in {g.home_team_id, g.away_team_id}:
        return False

    # Type filter
    gtype = (g.type or "").strip().lower()
    if include_day_types and gtype not in {t.lower() for t in include_day_types}:
        return False

    # Placeholder filter
    if not include_placeholders:
        if is_placeholder_team(g.home_team_name) or is_placeholder_team(g.away_team_name):
            return False

    return True


def should_include_game_for_master_calendar(
    g: Game,
    include_day_types: List[str],
    include_placeholders: bool,
) -> bool:
    gtype = (g.type or "").strip().lower()
    if include_day_types and gtype not in {t.lower() for t in include_day_types}:
        return False

    if not include_placeholders:
        if is_placeholder_team(g.home_team_name) or is_placeholder_team(g.away_team_name):
            return False

    return True


def write_ics(path: str, cal_name: str, tz_name: str, events: List[List[str]]) -> None:
    lines: List[str] = []
    lines.extend(calendar_header(cal_name, tz_name))
    for ev in events:
        lines.extend(ev)
    lines.extend(calendar_footer())
    content = "\r\n".join(lines) + "\r\n"

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    s = re.sub(r"-+", "-", s)
    return s


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    config_path = os.environ.get("CONFIG", DEFAULT_CONFIG_PATH)
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    output_dir = cfg.get("output_dir", "docs")
    tz_name = cfg.get("default_timezone", "America/New_York")

    season_year = int(cfg.get("season_year", 2025))

    opponent_recent_limit = int(cfg.get("opponent_recent_limit", 10))
    include_placeholders = bool(cfg.get("include_placeholder_games", True))
    include_division_in_summary = bool(cfg.get("include_division_in_event_name", False))

    # Team calendars day-types default: game, make_up, scrimmage
    include_day_types_team = cfg.get("include_day_types_team", ["game", "make_up", "scrimmage"])
    # Master calendar day-types default: game, make_up, scrimmage (configurable separately)
    include_day_types_master = cfg.get("include_day_types_master", ["game", "make_up", "scrimmage"])

    # 1) Lookup season id by year
    seasons_payload = http_get_json(SEASONS_URL)
    seasons = extract_results(seasons_payload)
    season_id: Optional[int] = None
    for s in seasons:
        if not isinstance(s, dict):
            continue
        if int(s.get("year")) == season_year:
            season_id = int(s.get("id"))
            break
    if season_id is None:
        raise SystemExit(f"Could not find season_id for year={season_year} from {SEASONS_URL}")

    # 2) Registered teams + divisions for that season
    regs_payload = http_get_json(TEAM_REGS_URL_TMPL.format(season_id=season_id))
    regs = extract_results(regs_payload)

    teams: List[Team] = []
    team_by_id: Dict[int, Team] = {}
    for r in regs:
        if not isinstance(r, dict):
            continue
        t = r.get("team") or {}
        d = r.get("division") or {}
        try:
            tid = int(t.get("id"))
        except Exception:
            continue
        team = Team(
            id=tid,
            name=normalize_str(t.get("name")),
            division_name=normalize_str(d.get("name")),
            division_short=normalize_str(d.get("short_name")),
        )
        teams.append(team)
        team_by_id[team.id] = team

    # 3) Schedule/results (source of truth)
    game_days_payload = http_get_json(GAME_DAYS_URL_TMPL.format(season_id=season_id))
    all_games = parse_games(game_days_payload, tz_name=tz_name, default_duration_min=int(cfg.get("default_game_duration_min", 60)))

    # 4) Build master calendar events
    master_events: List[List[str]] = []
    for g in all_games:
        if not should_include_game_for_master_calendar(g, include_day_types_master, include_placeholders):
            continue

        # DTSTART/DTEND — if no dt, make it all-day placeholder on "day" if present; but game object should have dt
        dtstart = g.dt_local
        dtend = (dtstart + timedelta(minutes=g.duration_min)) if dtstart else None

        uid = stable_uid("master", str(season_year), str(g.id))
        summary = calendar_summary_for_master_game(g)
        location = format_location(g.location, g.court)

        # Master calendar doesn't include head-to-head sections (no single team POV)
        desc: List[str] = []
        desc.extend(ascii_rule("GAME INFO"))
        desc.append(f"Season: {season_year}")
        desc.append(f"Stage: {g.type}")
        desc.append(f"Status: {g.status}")
        if dtstart:
            desc.append(f"Start ({tz_name}): {format_dt_local(dtstart, tz_name)}")
        if location:
            desc.append(f"Rink: {location}")
        desc.append(f"Check-in / Standings: {BTSH_HOME_URL}")

        master_events.append(
            ics_event(
                uid=uid,
                summary=summary,
                dtstart_local=dtstart,
                dtend_local=dtend,
                tz_name=tz_name,
                description_lines=desc,
                location=location,
                url=BTSH_HOME_URL,
            )
        )

    # Write master
    master_path = os.path.join(output_dir, f"btsh-all-games-season-{season_year}.ics")
    write_ics(master_path, f"BTSH All Games ({season_year})", tz_name, master_events)

    # 5) Per-team calendars
    for team in teams:
        events: List[List[str]] = []

        # team games are those where team is home/away
        for g in all_games:
            if not should_include_game_for_team_calendar(g, team.id, include_day_types_team, include_placeholders):
                continue

            # opponent resolved
            if team.id == g.home_team_id:
                opp_id = g.away_team_id
                opp_name = g.away_team_name
            else:
                opp_id = g.home_team_id
                opp_name = g.home_team_name

            opponent = team_by_id.get(opp_id) if opp_id is not None else None
            # If opponent not registered (placeholders), synthesize
            if opponent is None:
                opponent = Team(id=opp_id or -1, name=opp_name or "TBD")

            dtstart = g.dt_local
            dtend = (dtstart + timedelta(minutes=g.duration_min)) if dtstart else None

            uid = stable_uid("team", str(team.id), str(season_year), str(g.id))
            summary = calendar_summary_for_team_game(team, g, include_division_in_summary)

            location = format_location(g.location, g.court)
            desc = build_description(
                season_year=season_year,
                tz_name=tz_name,
                team=team,
                opponent=opponent,
                game=g,
                all_games=all_games,
                opponent_recent_limit=opponent_recent_limit,
            )

            events.append(
                ics_event(
                    uid=uid,
                    summary=summary,
                    dtstart_local=dtstart,
                    dtend_local=dtend,
                    tz_name=tz_name,
                    description_lines=desc,
                    location=location,
                    url=BTSH_HOME_URL,
                )
            )

        team_slug = slugify(team.name)
        team_path = os.path.join(output_dir, f"btsh-{team_slug}-season-{season_year}.ics")
        write_ics(team_path, f"BTSH {team.name} ({season_year})", tz_name, events)

    print(f"Wrote {len(teams)} team calendars + master to {output_dir}/ (season {season_year}, season_id={season_id})")


if __name__ == "__main__":
    main()