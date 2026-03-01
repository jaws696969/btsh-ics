#!/usr/bin/env python3
"""
BTSH ICS Generator

Flow:
- Read config.yml (season_year, output_dir, filtering, etc.)
- seasons API -> find season id for configured year
- team-season-registrations -> list of registered teams (+ divisions)
- game_days -> schedule/results (source of truth)
- Write one .ics per team + one master .ics for all games

Notes:
- Filters use RAW `type` fields (e.g. "game"), not get_type_display ("Game").
- Times are converted to America/New_York.
- Cancelled games are included and prefixed in SUMMARY with "CANCELLED:".
- Completed games include score "Away-Home" and (OT)/(SO) tag when result is final_ot/final_so.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml
from zoneinfo import ZoneInfo

SEASONS_URL = "https://api.btsh.org/api/seasons/"
TEAM_REGS_URL_TMPL = "https://api.btsh.org/api/team-season-registrations/?season={season_id}"
GAME_DAYS_URL_TMPL = "https://api.btsh.org/api/game_days/?season={season_id}"

CHECKIN_URL = "https://btsh.org"

PRODID = "-//btsh-ics//EN"
TZ_NAME_DEFAULT = "America/New_York"


# -------------------------
# Small utilities
# -------------------------

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "team"


def ordinal_day(n: int) -> str:
    # 1st, 2nd, 3rd, 4th, ...
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def fmt_short_date(dt_local: datetime) -> str:
    # "Aug 3rd"
    return f"{dt_local.strftime('%b')} {ordinal_day(dt_local.day)}"


def ensure_dir(path: str) -> None:
    import os
    os.makedirs(path, exist_ok=True)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


# -------------------------
# ICS helpers
# -------------------------

def fold_ics_line(line: str) -> List[str]:
    """
    RFC5545 line folding: lines longer than 75 octets should be folded.
    We approximate by chars (good enough for plain ASCII content we generate).
    """
    if len(line) <= 75:
        return [line]
    out = []
    cur = line
    out.append(cur[:75])
    cur = cur[75:]
    while cur:
        out.append(" " + cur[:74])
        cur = cur[74:]
    return out


def ics_escape(text: str) -> str:
    # Escape per RFC5545 for TEXT values
    text = text.replace("\\", "\\\\")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", "\\n")
    text = text.replace(";", r"\;")
    text = text.replace(",", r"\,")
    return text


def dt_local_ics(dt_local: datetime) -> str:
    # DTSTART;TZID=America/New_York:YYYYMMDDTHHMMSS
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
    desc = "\n".join(description_lines).strip()
    lines = [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        f"SUMMARY:{ics_escape(summary)}",
        f"DTSTART;VALUE=DATE:{start_date}",
        f"DTEND;VALUE=DATE:{end_date}",
        f"DESCRIPTION:{ics_escape(desc)}",
    ]
    if url:
        lines.append(f"URL:{ics_escape(url)}")
    lines.append("END:VEVENT")

    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded


def ics_calendar_header(calname: str, tz_name: str) -> List[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{PRODID}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(calname)}",
        f"X-WR-TIMEZONE:{tz_name}",
    ]


def ics_calendar_footer() -> List[str]:
    return ["END:VCALENDAR"]


# -------------------------
# Data models
# -------------------------

@dataclass(frozen=True)
class TeamReg:
    team_id: int
    team_name: str
    division_name: str  # e.g. "Division 3"
    division_short: str  # e.g. "3"


@dataclass
class GameRow:
    # Unique-ish key
    game_id: Optional[int]  # sometimes may not exist
    day_id: int
    type_raw: str                # "game", "make_up", "scrimmage", "holiday", ...
    type_display: str            # "Game", "Make up", ...
    status: str                  # "scheduled" | "completed" | "cancelled" ...
    day_local: datetime          # date at midnight in NY
    start_local: Optional[datetime]
    end_local: Optional[datetime]

    location: str
    court: str
    court_display: str

    home_team_id: Optional[int]
    home_team_name: str
    away_team_id: Optional[int]
    away_team_name: str

    # scores
    home_goals: Optional[int]
    away_goals: Optional[int]
    result_code: str  # "", "final", "final_ot", "final_so" etc.

    # day-level responsibilities
    opening_team_id: Optional[int]
    closing_team_id: Optional[int]


# -------------------------
# API fetch
# -------------------------

def fetch_json(url: str, timeout: int = 30) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def pick_season_id_for_year(seasons_payload: Any, year: int) -> int:
    # seasons endpoint seems to return a list
    for s in seasons_payload:
        if int(s.get("year")) == int(year):
            return int(s["id"])
    raise RuntimeError(f"Could not find season id for year={year} in seasons API.")


def parse_team_regs(payload: Any) -> List[TeamReg]:
    out: List[TeamReg] = []
    for row in payload.get("results", []):
        team = row.get("team") or {}
        div = row.get("division") or {}
        out.append(
            TeamReg(
                team_id=int(team.get("id")),
                team_name=str(team.get("name") or "").strip(),
                division_name=str(div.get("name") or "").strip(),
                division_short=str(div.get("short_name") or "").strip(),
            )
        )
    # stable ordering
    out.sort(key=lambda t: t.team_name.lower())
    return out


def parse_game_days(payload: Any, tz_name: str) -> List[GameRow]:
    tz = ZoneInfo(tz_name)
    rows: List[GameRow] = []

    for day in payload.get("results", []):
        day_id = int(day["id"])
        type_raw = str(day.get("type") or "").strip()          # IMPORTANT: raw
        type_display = str(day.get("get_type_display") or type_raw).strip()

        day_date_str = str(day.get("day") or "").strip()       # "YYYY-MM-DD"
        if not day_date_str:
            continue
        day_local = datetime.strptime(day_date_str, "%Y-%m-%d").replace(tzinfo=tz)

        opening_team = day.get("opening_team")
        closing_team = day.get("closing_team")
        opening_team_id = int(opening_team["id"]) if isinstance(opening_team, dict) and opening_team.get("id") else None
        closing_team_id = int(closing_team["id"]) if isinstance(closing_team, dict) and closing_team.get("id") else None

        games = day.get("games") or []
        if not games:
            # non-game day: represent it as all-day event row
            rows.append(
                GameRow(
                    game_id=None,
                    day_id=day_id,
                    type_raw=type_raw,
                    type_display=type_display,
                    status=str(day.get("status") or "").strip() or "scheduled",
                    day_local=day_local,
                    start_local=None,
                    end_local=None,
                    location=str(day.get("location") or "").strip(),
                    court=str(day.get("court") or "").strip(),
                    court_display=str(day.get("get_court_display") or "").strip(),
                    home_team_id=None,
                    home_team_name="",
                    away_team_id=None,
                    away_team_name="",
                    home_goals=None,
                    away_goals=None,
                    result_code="",
                    opening_team_id=opening_team_id,
                    closing_team_id=closing_team_id,
                )
            )
            continue

        for g in games:
            # Game fields
            g_type_raw = str(g.get("type") or type_raw).strip()
            g_type_display = str(g.get("get_type_display") or type_display or g_type_raw).strip()
            status = str(g.get("status") or "").strip() or str(day.get("status") or "").strip() or "scheduled"

            start_str = str(g.get("start") or "").strip()  # "HH:MM:SS" (local)
            end_str = str(g.get("end") or "").strip()

            start_local = None
            end_local = None
            if start_str:
                # combine with day_local date
                t = datetime.strptime(start_str, "%H:%M:%S").time()
                start_local = datetime.combine(day_local.date(), t, tzinfo=tz)
            if end_str:
                t2 = datetime.strptime(end_str, "%H:%M:%S").time()
                end_local = datetime.combine(day_local.date(), t2, tzinfo=tz)

            # if end missing, guess 1 hour
            if start_local and not end_local:
                end_local = start_local + timedelta(hours=1)

            home = g.get("home_team") or {}
            away = g.get("away_team") or {}

            home_id = home.get("id")
            away_id = away.get("id")

            home_goals = g.get("home_team_num_goals")
            away_goals = g.get("away_team_num_goals")

            # normalize scores to int/None
            try:
                home_goals = int(home_goals) if home_goals is not None else None
            except Exception:
                home_goals = None
            try:
                away_goals = int(away_goals) if away_goals is not None else None
            except Exception:
                away_goals = None

            result_code = str(g.get("result") or "").strip()

            location = str(g.get("location") or day.get("location") or "").strip()
            court = str(g.get("court") or day.get("court") or "").strip()
            court_display = str(g.get("get_court_display") or day.get("get_court_display") or "").strip()

            rows.append(
                GameRow(
                    game_id=int(g["id"]) if g.get("id") is not None else None,
                    day_id=day_id,
                    type_raw=g_type_raw,
                    type_display=g_type_display,
                    status=status,
                    day_local=day_local,
                    start_local=start_local,
                    end_local=end_local,
                    location=location,
                    court=court,
                    court_display=court_display,
                    home_team_id=int(home_id) if home_id is not None else None,
                    home_team_name=str(home.get("name") or "").strip(),
                    away_team_id=int(away_id) if away_id is not None else None,
                    away_team_name=str(away.get("name") or "").strip(),
                    home_goals=home_goals,
                    away_goals=away_goals,
                    result_code=result_code,
                    opening_team_id=opening_team_id,
                    closing_team_id=closing_team_id,
                )
            )

    # sort by start time, then all-day days
    def sort_key(r: GameRow):
        return (
            r.start_local or r.day_local,
            r.home_team_name.lower(),
            r.away_team_name.lower(),
            r.day_id,
            r.game_id or 0,
        )

    rows.sort(key=sort_key)
    return rows


# -------------------------
# Formatting for descriptions
# -------------------------

def ascii_rule(title: str) -> List[str]:
    return [
        "-" * 40,
        title,
        "-" * 40,
    ]


def rink_label(row: GameRow) -> str:
    # example: "Tompkins Square Park (West)" from location + court_display
    loc = row.location.strip()
    court_disp = row.court_display.strip()
    if loc and court_disp:
        return f"{loc} ({court_disp})"
    return loc or court_disp or ""


def score_suffix(row: GameRow) -> str:
    # Only for completed games with scores
    if row.status != "completed":
        return ""
    if row.away_goals is None or row.home_goals is None:
        return ""
    # base score Away-Home
    base = f"{row.away_goals}-{row.home_goals}"
    tag = ""
    if row.result_code == "final_ot":
        tag = " (OT)"
    elif row.result_code == "final_so":
        tag = " (SO)"
    return base + tag


def opponent_from_calendar_team(row: GameRow, team_id: int) -> Tuple[str, bool]:
    # Returns (opponent_name, calendar_team_is_home)
    if row.home_team_id == team_id:
        return row.away_team_name, True
    if row.away_team_id == team_id:
        return row.home_team_name, False
    return "", False


def is_placeholder_game(row: GameRow) -> bool:
    # Placeholder is when teams are missing or TBD-like; treat "-" or "TBD" as placeholder.
    names = (row.home_team_name or "", row.away_team_name or "")
    if not names[0].strip() or not names[1].strip():
        return True
    bad = {"-", "tbd", "t.b.d", "to be determined"}
    return names[0].strip().lower() in bad or names[1].strip().lower() in bad


def format_team_line(row: GameRow, perspective_team_id: Optional[int], when: datetime) -> str:
    """
    For opponent schedule + head-to-head lines:
      "Aug 3rd @ Poutine Machine (L 2-11)" style BUT per your instruction:
      - don't include W/L, only score Away-Home
      - if cancelled: "(Cancelled)"
      - if no scores: no parentheses
    perspective_team_id: the team we're writing the list for (opponent team id)
    """
    # Determine @ vs relative to perspective team
    if perspective_team_id is None:
        # fallback
        vs_str = f"@ {row.home_team_name}" if row.away_team_name else "vs ?"
    else:
        if row.away_team_id == perspective_team_id:
            # perspective is away
            opp = row.home_team_name
            prefix = "@"
        elif row.home_team_id == perspective_team_id:
            opp = row.away_team_name
            prefix = "vs"
        else:
            # not expected
            opp = row.home_team_name or row.away_team_name
            prefix = "vs"

        vs_str = f"{prefix} {opp}"

    # date
    dt = row.start_local or row.day_local
    date_part = fmt_short_date(dt)

    # status/score
    extra = ""
    if row.status == "cancelled":
        extra = "(Cancelled)"
    elif row.status == "completed":
        sc = score_suffix(row)
        if sc:
            extra = f"({sc})"

    return f"    {date_part} {vs_str}" + (f" {extra}" if extra else "")


def compute_record_completed_only(games: List[GameRow], team_id: int, cutoff: datetime) -> Tuple[int, int]:
    w = 0
    l = 0
    for g in games:
        if not g.start_local:
            continue
        if g.start_local >= cutoff:
            continue
        if g.status != "completed":
            continue
        if g.away_goals is None or g.home_goals is None:
            continue

        if g.home_team_id == team_id:
            team_goals = g.home_goals
            opp_goals = g.away_goals
        elif g.away_team_id == team_id:
            team_goals = g.away_goals
            opp_goals = g.home_goals
        else:
            continue

        if team_goals > opp_goals:
            w += 1
        elif team_goals < opp_goals:
            l += 1
        # ties not expected
    return w, l


def build_description_for_team_game(
    season_year: int,
    tz_name: str,
    calendar_team: TeamReg,
    row: GameRow,
    all_games: List[GameRow],
) -> List[str]:
    tz = ZoneInfo(tz_name)
    start_local = row.start_local.astimezone(tz) if row.start_local else None

    rink = rink_label(row)

    # opponent
    opp_name, _calendar_team_is_home = opponent_from_calendar_team(row, calendar_team.team_id)
    opp_name_up = opp_name.upper().strip()

    # --- GAME INFO ---
    lines: List[str] = []
    lines.extend(ascii_rule("GAME INFO"))
    lines.append(f"Season: {season_year}")
    lines.append(f"Stage: {row.type_display}")
    lines.append(f"Status: {row.status}")
    if start_local:
        # "2025-10-26 15:45 EDT"
        lines.append(f"Start ({tz_name}): {start_local.strftime('%Y-%m-%d %H:%M %Z')}")
    if rink:
        lines.append(f"Rink: {rink}")
    lines.append(f"Check-in / Standings: {CHECKIN_URL}")
    lines.append("")

    # --- HEAD TO HEAD ---
    lines.extend(ascii_rule(f"HEAD-TO-HEAD vs {opp_name}"))
    cutoff = row.start_local or row.day_local.replace(tzinfo=tz)

    prior_h2h = []
    for g in all_games:
        if not g.start_local:
            continue
        if g.start_local >= cutoff:
            continue
        teams = {g.home_team_id, g.away_team_id}
        if calendar_team.team_id in teams and (row.home_team_id in teams and row.away_team_id in teams):
            prior_h2h.append(g)

    if not prior_h2h:
        lines.append("    (no prior matchups listed)")
    else:
        for g in prior_h2h:
            lines.append(format_team_line(g, calendar_team.team_id, cutoff))
    lines.append("")

    # --- OPPONENT GAMES TO DATE ---
    lines.extend(ascii_rule(f"{opp_name_up} GAMES-TO-DATE"))

    # collect opponent games (prior to this event)
    opp_games_prior = []
    opp_id = row.home_team_id if row.home_team_id != calendar_team.team_id else row.away_team_id
    opp_id = opp_id if opp_id is not None else None

    for g in all_games:
        if not g.start_local:
            continue
        if g.start_local >= cutoff:
            continue
        if opp_id is not None and (g.home_team_id == opp_id or g.away_team_id == opp_id):
            opp_games_prior.append(g)

    if opp_id is not None:
        w, l = compute_record_completed_only(all_games, opp_id, cutoff)
        lines.append(f"Record to date (completed games only): {w}-{l}")

    if not opp_games_prior:
        lines.append("    (no prior games listed)")
    else:
        # show in chronological order
        for g in opp_games_prior:
            lines.append(format_team_line(g, opp_id, cutoff))

    return lines


# -------------------------
# SUMMARY formatting
# -------------------------

def build_summary_team_calendar(row: GameRow, calendar_team: TeamReg) -> str:
    """
    Team calendar:
      "{Team} vs {Opponent}" or "{Team} @ {Opponent}"
    Include:
      - CANCELLED prefix if cancelled
      - [Opening]/[Closing] if responsible
      - score suffix if completed
    """
    opp_name, team_is_home = opponent_from_calendar_team(row, calendar_team.team_id)
    if not opp_name:
        # fallback
        base = f"{row.away_team_name} @ {row.home_team_name}".strip()
    else:
        if team_is_home:
            base = f"{calendar_team.team_name} vs {opp_name}"
        else:
            base = f"{calendar_team.team_name} @ {opp_name}"

    tags: List[str] = []
    if row.opening_team_id == calendar_team.team_id:
        tags.append("[Opening]")
    if row.closing_team_id == calendar_team.team_id:
        tags.append("[Closing]")

    sc = score_suffix(row)
    if sc:
        base = f"{base} ({sc})"

    if tags:
        base = f"{base} " + " ".join(tags)

    if row.status == "cancelled":
        base = f"CANCELLED: {base}"

    return base


def build_summary_master(row: GameRow) -> str:
    """
    Master calendar:
      "{Away} @ {Home}"
    Include:
      - CANCELLED prefix if cancelled
      - score suffix if completed
    """
    base = f"{row.away_team_name} @ {row.home_team_name}".strip()
    sc = score_suffix(row)
    if sc:
        base = f"{base} ({sc})"
    if row.status == "cancelled":
        base = f"CANCELLED: {base}"
    return base


# -------------------------
# Main generation
# -------------------------

def load_config(path: str = "config.yml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg


def write_ics(path: str, calname: str, tz_name: str, event_lines: List[str]) -> None:
    lines = []
    lines.extend(ics_calendar_header(calname, tz_name))
    lines.extend(event_lines)
    lines.extend(ics_calendar_footer())
    content = "\r\n".join(lines) + "\r\n"
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)


def main() -> None:
    cfg = load_config("config.yml")

    output_dir = str(cfg.get("output_dir") or "docs")
    tz_name = str(cfg.get("default_timezone") or TZ_NAME_DEFAULT)

    season_year = int(cfg.get("season_year") or cfg.get("season") or 2025)

    # which day types to include
    team_day_types = set((cfg.get("team_day_types") or ["game", "make_up", "scrimmage"]))
    master_day_types = set((cfg.get("master_day_types") or ["game", "make_up", "scrimmage", "holiday", "other"]))

    include_placeholders = bool(cfg.get("include_placeholders", True))

    ensure_dir(output_dir)

    # fetch live data
    seasons = fetch_json(SEASONS_URL)
    season_id = pick_season_id_for_year(seasons, season_year)

    team_regs_payload = fetch_json(TEAM_REGS_URL_TMPL.format(season_id=season_id))
    team_regs = parse_team_regs(team_regs_payload)

    game_days_payload = fetch_json(GAME_DAYS_URL_TMPL.format(season_id=season_id))
    all_rows = parse_game_days(game_days_payload, tz_name)

    # only keep actual games for team calendars/master calendars
    # (non-game days are rows where start_local is None)
    # filtering is based on row.type_raw, which is raw api "type" (lowercase).
    def row_allowed_for(types_set: set[str], r: GameRow) -> bool:
        if r.type_raw not in types_set:
            return False
        if (r.home_team_name.strip() == "" and r.away_team_name.strip() == "") and r.start_local is not None:
            return False
        if not include_placeholders and is_placeholder_game(r):
            return False
        return True

    # master calendar events
    master_events: List[str] = []
    for r in all_rows:
        if not row_allowed_for(master_day_types, r):
            continue

        if r.start_local and r.end_local and r.home_team_name and r.away_team_name:
            uid = f"btsh:{season_year}:day{r.day_id}:game{r.game_id or sha1(r.away_team_name + r.home_team_name + str(r.start_local))}"
            summary = build_summary_master(r)

            desc_lines: List[str] = []
            desc_lines.extend(ascii_rule("GAME INFO"))
            desc_lines.append(f"Season: {season_year}")
            desc_lines.append(f"Stage: {r.type_display}")
            desc_lines.append(f"Status: {r.status}")
            start_local = r.start_local.astimezone(ZoneInfo(tz_name))
            desc_lines.append(f"Start ({tz_name}): {start_local.strftime('%Y-%m-%d %H:%M %Z')}")
            rink = rink_label(r)
            if rink:
                desc_lines.append(f"Rink: {rink}")
            desc_lines.append(f"Check-in / Standings: {CHECKIN_URL}")

            ev = ics_event(
                uid=uid,
                summary=summary,
                dtstart_local=r.start_local,
                dtend_local=r.end_local,
                tz_name=tz_name,
                description_lines=desc_lines,
                location=rink_label(r),
                url=CHECKIN_URL,
            )
            master_events.extend(ev)
        else:
            # all-day non-game event
            uid = f"btsh:{season_year}:day{r.day_id}:allday:{r.type_raw}"
            summary = f"{r.type_display} Day"
            if r.status == "cancelled":
                summary = f"CANCELLED: {summary}"
            desc_lines = []
            desc_lines.extend(ascii_rule("DAY INFO"))
            desc_lines.append(f"Season: {season_year}")
            desc_lines.append(f"Stage: {r.type_display}")
            desc_lines.append(f"Status: {r.status}")
            desc_lines.append(f"Check-in / Standings: {CHECKIN_URL}")
            ev = ics_allday_event(uid, summary, r.day_local, desc_lines, url=CHECKIN_URL)
            master_events.extend(ev)

    master_path = f"{output_dir}/btsh-all-games-season-{season_year}.ics"
    write_ics(master_path, f"BTSH All Games ({season_year})", tz_name, master_events)

    # team calendars
    for team in team_regs:
        events: List[str] = []

        for r in all_rows:
            if not row_allowed_for(team_day_types, r):
                continue

            # include:
            # - games where this team is home or away
            # - optionally all-day league events? only if configured types include them and r.start_local is None
            is_team_game = (r.home_team_id == team.team_id) or (r.away_team_id == team.team_id)
            is_all_day = r.start_local is None

            if not is_team_game and not is_all_day:
                continue

            if is_all_day:
                # include all-day event on every team calendar if type allowed
                uid = f"btsh:{season_year}:team{team.team_id}:day{r.day_id}:allday:{r.type_raw}"
                summary = f"{r.type_display} Day"
                desc_lines = []
                desc_lines.extend(ascii_rule("DAY INFO"))
                desc_lines.append(f"Season: {season_year}")
                desc_lines.append(f"Stage: {r.type_display}")
                desc_lines.append(f"Status: {r.status}")
                desc_lines.append(f"Check-in / Standings: {CHECKIN_URL}")

                ev = ics_allday_event(uid, summary, r.day_local, desc_lines, url=CHECKIN_URL)
                events.extend(ev)
                continue

            # timed game
            uid = f"btsh:{season_year}:team{team.team_id}:day{r.day_id}:game{r.game_id or sha1(team.team_name + str(r.start_local))}"

            summary = build_summary_team_calendar(r, team)
            desc_lines = build_description_for_team_game(
                season_year=season_year,
                tz_name=tz_name,
                calendar_team=team,
                row=r,
                all_games=all_rows,
            )

            ev = ics_event(
                uid=uid,
                summary=summary,
                dtstart_local=r.start_local,
                dtend_local=r.end_local,
                tz_name=tz_name,
                description_lines=desc_lines,
                location=rink_label(r),
                url=CHECKIN_URL,
            )
            events.extend(ev)

        calname = f"BTSH {team.team_name} ({season_year})"
        out_path = f"{output_dir}/btsh-{slugify(team.team_name)}-season-{season_year}.ics"
        write_ics(out_path, calname, tz_name, events)

    print(f"Wrote {len(team_regs)} team calendars + master calendar for season {season_year} into {output_dir}/")


if __name__ == "__main__":
    main()