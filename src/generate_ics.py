#!/usr/bin/env python3
"""
BTSH ICS Generator

What this script does (season-year driven):
- Config specifies a season YEAR (e.g., 2026), NOT the season id.
- Looks up the season id via https://api.btsh.org/api/seasons/
- Fetches:
  - game days: https://api.btsh.org/api/game_days/?season=<season_id>
  - team registrations: https://api.btsh.org/api/team-season-registrations/?season=<season_id>
- Generates:
  - one .ics per registered team (only teams registered that season)
  - one "all games" .ics for that season
- Event description includes:
  - GAME INFO block
  - HEAD-TO-HEAD (prior matchups only)
  - OPPONENT RECORD-TO-DATE (as of event start, based on known results)
  - OPPONENT GAMES-TO-DATE (all games with start < event start; include results if present, otherwise no result)
  - BTSH link for game check-in/registration

Notes:
- Cancelled games are INCLUDED and labeled.
- Placeholder/non-game items (rainout/playoffs placeholders) are INCLUDED and labeled where detectable.
- Win type display tries to detect OT/SO from status_display (e.g., "Final (OT)", "Final (SO)") when present.

Requires: requests, pyyaml
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml
from zoneinfo import ZoneInfo


# ----------------------------
# Config
# ----------------------------

DEFAULT_CONFIG_PATH = os.environ.get("BTSH_CONFIG", "config.yml")


# ----------------------------
# Small helpers
# ----------------------------

def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def safe_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "team"


def ascii_rule(title: str, width: int = 40) -> List[str]:
    line = "-" * width
    title = title.strip()
    return [line, title, line]


def ordinal(n: int) -> str:
    # 1st, 2nd, 3rd, 4th...
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def fmt_short_date_local(dt_local: datetime) -> str:
    # e.g. "Feb 3rd"
    return f"{dt_local:%b} {ordinal(dt_local.day)}"


def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # API seems to return offset-aware datetimes like 2025-03-03T19:24:22.231580-05:00
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_ics(dt_local: datetime) -> str:
    # Use floating time with explicit TZID in DTSTART/DTEND lines; keep local wall time
    return dt_local.strftime("%Y%m%dT%H%M%S")


def stable_uid(parts: List[str]) -> str:
    raw = "||".join(parts)
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{h}@btsh-ics"


def ics_escape(s: str) -> str:
    # RFC5545 escaping
    return (
        s.replace("\\", "\\\\")
         .replace("\r\n", "\n")
         .replace("\r", "\n")
         .replace("\n", "\\n")
         .replace(",", "\\,")
         .replace(";", "\\;")
    )


def fold_ics_line(line: str, limit: int = 75) -> List[str]:
    # RFC5545 line folding: after 75 octets; we approximate by chars (good enough for ASCII)
    if len(line) <= limit:
        return [line]
    out = []
    while len(line) > limit:
        out.append(line[:limit])
        line = " " + line[limit:]
    out.append(line)
    return out


def write_ics(path: str, lines: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for ln in lines:
            for folded in fold_ics_line(ln):
                f.write(folded + "\n")


# ----------------------------
# Domain models
# ----------------------------

@dataclass(frozen=True)
class TeamReg:
    team_id: int
    team_name: str
    division_name: Optional[str]
    division_short: Optional[str]


@dataclass(frozen=True)
class Game:
    # Core
    start_utc: Optional[datetime]
    end_utc: Optional[datetime]
    location: Optional[str]
    status: Optional[str]
    status_display: Optional[str]
    stage_display: Optional[str]

    # Teams
    home_team_id: Optional[int]
    away_team_id: Optional[int]
    home_team_name: Optional[str]
    away_team_name: Optional[str]

    # Score
    home_goals: Optional[int]
    away_goals: Optional[int]

    # Other
    raw: Dict[str, Any]


# ----------------------------
# BTSH API
# ----------------------------

def fetch_json(url: str, timeout_s: int = 30) -> Any:
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def btsh_get_season_id_for_year(year: int) -> int:
    seasons = fetch_json("https://api.btsh.org/api/seasons/?")
    if not isinstance(seasons, list):
        die("Unexpected seasons response (expected a list).")
    matches = [s for s in seasons if isinstance(s, dict) and s.get("year") == year]
    if not matches:
        die(f"No season found with year={year}.")
    # If multiple, pick the one that looks most "current" (latest start date)
    def sort_key(s: dict) -> Tuple[int, str]:
        start = s.get("start") or ""
        return (int(s.get("id") or 0), str(start))
    matches.sort(key=sort_key, reverse=True)
    sid = matches[0].get("id")
    if not isinstance(sid, int):
        die("Season match found but missing integer 'id'.")
    return sid


def btsh_fetch_team_registrations(season_id: int) -> List[TeamReg]:
    data = fetch_json(f"https://api.btsh.org/api/team-season-registrations/?season={season_id}")
    # Based on your sample: {"results":[...]}
    results = data.get("results") if isinstance(data, dict) else None
    if not isinstance(results, list):
        die("Unexpected team-season-registrations response (missing results list).")

    out: List[TeamReg] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        team = row.get("team") or {}
        div = row.get("division") or {}
        team_id = team.get("id")
        team_name = team.get("name")
        if not isinstance(team_id, int) or not isinstance(team_name, str):
            continue
        out.append(
            TeamReg(
                team_id=team_id,
                team_name=team_name.strip(),
                division_name=(div.get("name") if isinstance(div.get("name"), str) else None),
                division_short=(div.get("short_name") if isinstance(div.get("short_name"), str) else None),
            )
        )
    # De-dupe by team_id (keep first)
    seen = set()
    uniq: List[TeamReg] = []
    for t in out:
        if t.team_id in seen:
            continue
        seen.add(t.team_id)
        uniq.append(t)
    return uniq


def btsh_fetch_game_days(season_id: int) -> List[dict]:
    data = fetch_json(f"https://api.btsh.org/api/game_days/?season={season_id}")
    if not isinstance(data, list):
        die("Unexpected game_days response (expected list).")
    return data


def parse_games_from_days(days: List[dict]) -> List[Game]:
    games: List[Game] = []
    for day in days:
        if not isinstance(day, dict):
            continue
        for g in (day.get("games") or []):
            if not isinstance(g, dict):
                continue

            start_dt = parse_iso_dt(g.get("start_datetime") or g.get("start") or g.get("start_time"))
            end_dt = parse_iso_dt(g.get("end_datetime") or g.get("end") or g.get("end_time"))

            home = g.get("home_team") if isinstance(g.get("home_team"), dict) else {}
            away = g.get("away_team") if isinstance(g.get("away_team"), dict) else {}

            def get_team_id(t: Any) -> Optional[int]:
                if isinstance(t, dict) and isinstance(t.get("id"), int):
                    return t["id"]
                return None

            def get_team_name(t: Any) -> Optional[str]:
                if isinstance(t, dict) and isinstance(t.get("name"), str):
                    return t["name"].strip()
                return None

            home_goals = g.get("home_team_num_goals")
            away_goals = g.get("away_team_num_goals")
            home_goals = home_goals if isinstance(home_goals, int) else None
            away_goals = away_goals if isinstance(away_goals, int) else None

            games.append(
                Game(
                    start_utc=start_dt,
                    end_utc=end_dt,
                    location=(g.get("location") if isinstance(g.get("location"), str) else None),
                    status=(g.get("status") if isinstance(g.get("status"), str) else None),
                    status_display=(g.get("status_display") if isinstance(g.get("status_display"), str) else None),
                    stage_display=(g.get("stage_display") if isinstance(g.get("stage_display"), str) else None),
                    home_team_id=get_team_id(home),
                    away_team_id=get_team_id(away),
                    home_team_name=get_team_name(home),
                    away_team_name=get_team_name(away),
                    home_goals=home_goals,
                    away_goals=away_goals,
                    raw=g,
                )
            )
    # stable sort by start
    def k(x: Game) -> Tuple[int, str]:
        if x.start_utc is None:
            return (1, "")
        return (0, x.start_utc.isoformat())
    games.sort(key=k)
    return games


# ----------------------------
# Result labeling (Reg / OT / SO)
# ----------------------------

def outcome_suffix_from_status_display(status_display: Optional[str]) -> str:
    if not status_display:
        return ""
    s = status_display.upper()
    # common patterns: "FINAL (OT)", "FINAL (SO)", sometimes "OT" / "SO" anywhere
    if "SO" in s or "SHOOT" in s:
        return " (SO)"
    if "OT" in s or "OVERTIME" in s:
        return " (OT)"
    return ""


def winner_for_game(g: Game) -> Optional[str]:
    # Returns "HOME", "AWAY", "TIE", or None if unknown
    if g.home_goals is None or g.away_goals is None:
        return None
    if g.home_goals > g.away_goals:
        return "HOME"
    if g.away_goals > g.home_goals:
        return "AWAY"
    return "TIE"


def result_token_for_team(g: Game, team_id: int) -> Optional[str]:
    win = winner_for_game(g)
    if win is None:
        return None

    suffix = outcome_suffix_from_status_display(g.status_display)

    if win == "TIE":
        return "T"

    if team_id == g.home_team_id:
        return ("W" if win == "HOME" else "L") + suffix
    if team_id == g.away_team_id:
        return ("W" if win == "AWAY" else "L") + suffix
    return None


def compute_record_to_date(games: List[Game], team_id: int) -> Dict[str, int]:
    """
    Computes record counts from games where scores are present.
    Breaks out regulation vs OT vs SO based on status_display heuristics.
    """
    rec = {
        "W": 0, "L": 0, "T": 0,
        "W_REG": 0, "L_REG": 0,
        "W_OT": 0, "L_OT": 0,
        "W_SO": 0, "L_SO": 0,
    }
    for g in games:
        tok = result_token_for_team(g, team_id)
        if tok is None:
            continue
        if tok == "T":
            rec["T"] += 1
            continue
        if tok.startswith("W"):
            rec["W"] += 1
            if "(SO)" in tok:
                rec["W_SO"] += 1
            elif "(OT)" in tok:
                rec["W_OT"] += 1
            else:
                rec["W_REG"] += 1
        elif tok.startswith("L"):
            rec["L"] += 1
            if "(SO)" in tok:
                rec["L_SO"] += 1
            elif "(OT)" in tok:
                rec["L_OT"] += 1
            else:
                rec["L_REG"] += 1
    return rec


def format_record_line(rec: Dict[str, int]) -> str:
    base = f"{rec['W']}-{rec['L']}-{rec['T']}"
    extras = f"Reg {rec['W_REG']}-{rec['L_REG']}, OT {rec['W_OT']}-{rec['L_OT']}, SO {rec['W_SO']}-{rec['L_SO']}"
    return f"Record-to-date: {base} ({extras})"


# ----------------------------
# Filtering / labeling placeholders & cancelled
# ----------------------------

def is_cancelled(g: Game) -> bool:
    if not g.status:
        return False
    return g.status.lower() in {"cancelled", "canceled"} or "cancel" in g.status.lower()


def is_placeholder(g: Game) -> bool:
    # Heuristic: missing teams OR status_display contains "TBD" OR stage suggests placeholder weeks
    sd = (g.status_display or "").upper()
    if "TBD" in sd or "PLACEHOLDER" in sd or "RAIN" in sd:
        return True
    if g.home_team_id is None and g.away_team_id is None:
        return True
    if (g.home_team_name or "").strip().upper() in {"TBD", "TBA"}:
        return True
    if (g.away_team_name or "").strip().upper() in {"TBD", "TBA"}:
        return True
    return False


# ----------------------------
# Event formatting
# ----------------------------

def team_side_and_opp(g: Game, team_id: int) -> Tuple[str, str, Optional[int]]:
    """
    Returns (is_home_or_away, opponent_name, opponent_id)
    """
    if g.home_team_id == team_id:
        return ("HOME", g.away_team_name or "TBD", g.away_team_id)
    if g.away_team_id == team_id:
        return ("AWAY", g.home_team_name or "TBD", g.home_team_id)
    # not actually involving team (shouldn't happen for team calendars)
    return ("UNKNOWN", "TBD", None)


def summary_for_team_game(
    g: Game,
    team_id: int,
    tz: ZoneInfo,
    team_div_short: Optional[str],
    opp_div_short: Optional[str],
    include_division: bool,
) -> str:
    side, opp_name, _ = team_side_and_opp(g, team_id)
    at = "@ " if side == "AWAY" else "vs "
    div_bit = ""
    if include_division:
        bits = []
        if team_div_short:
            bits.append(f"D{team_div_short}")
        if opp_div_short:
            bits.append(f"opp D{opp_div_short}")
        if bits:
            div_bit = f" [{' / '.join(bits)}]"
    if is_cancelled(g):
        return f"{at}{opp_name}{div_bit} (CANCELLED)"
    if is_placeholder(g):
        return f"{at}{opp_name}{div_bit} (PLACEHOLDER)"
    return f"{at}{opp_name}{div_bit}"


def summary_for_all_game(
    g: Game,
    include_division: bool,
    div_map: Dict[int, Optional[str]],
) -> str:
    h = g.home_team_name or "TBD"
    a = g.away_team_name or "TBD"
    div_bit = ""
    if include_division and g.home_team_id and g.away_team_id:
        hd = div_map.get(g.home_team_id)
        ad = div_map.get(g.away_team_id)
        if hd or ad:
            div_bit = f" [D{hd or '?'} vs D{ad or '?'}]"
    base = f"{h} vs {a}{div_bit}"
    if is_cancelled(g):
        return base + " (CANCELLED)"
    if is_placeholder(g):
        return base + " (PLACEHOLDER)"
    return base


def format_game_line_for_team(g: Game, team_id: int, tz: ZoneInfo) -> str:
    # "Feb 3rd vs Bulldogs (W 8-6)" or "Feb 6th @ Blizzard" (no result)
    if not g.start_utc:
        date_part = "TBD"
    else:
        dt_local = g.start_utc.astimezone(tz)
        date_part = fmt_short_date_local(dt_local)

    side, opp_name, _ = team_side_and_opp(g, team_id)
    vsat = "@ " if side == "AWAY" else "vs "
    tok = result_token_for_team(g, team_id)

    if is_cancelled(g):
        return f"    {date_part} {vsat}{opp_name} (CANCELLED)"

    if tok and g.home_goals is not None and g.away_goals is not None:
        # show score as team_score-opponent_score
        if team_id == g.home_team_id:
            sc = f"{g.home_goals}-{g.away_goals}"
        else:
            sc = f"{g.away_goals}-{g.home_goals}"
        return f"    {date_part} {vsat}{opp_name} ({tok} {sc})"
    else:
        # no result yet
        return f"    {date_part} {vsat}{opp_name}"


def build_description_for_team_event(
    g: Game,
    season_name: str,
    tz: ZoneInfo,
    team_id: int,
    team_name: str,
    opp_name: str,
    opp_id: Optional[int],
    all_games: List[Game],
    checkin_url: str,
) -> str:
    desc: List[str] = []

    # GAME INFO
    desc.extend(ascii_rule("GAME INFO"))
    desc.append(f"Season: {season_name}")
    desc.append(f"Stage: {g.stage_display or 'Unknown'}")
    desc.append(f"Status: {g.status or 'Unknown'}")

    if g.start_utc:
        dt_local = g.start_utc.astimezone(tz)
        desc.append(f"Start ({tz.key}): {dt_local:%Y-%m-%d %H:%M %Z}")
    else:
        desc.append(f"Start ({tz.key}): TBD")

    if g.location:
        desc.append(f"Location: {g.location}")

    # HEAD-TO-HEAD (prior matchups only)
    desc.append("")
    desc.extend(ascii_rule(f"HEAD-TO-HEAD vs {opp_name}".upper()))
    if g.start_utc and opp_id is not None:
        cutoff = g.start_utc
        prior = [
            gg for gg in all_games
            if gg.start_utc and gg.start_utc < cutoff
            and (
                (gg.home_team_id == team_id and gg.away_team_id == opp_id) or
                (gg.away_team_id == team_id and gg.home_team_id == opp_id)
            )
        ]
        if prior:
            for gg in prior:
                desc.append(format_game_line_for_team(gg, team_id, tz))
        else:
            desc.append("    (none)")
    else:
        desc.append("    (none)")

    # OPPONENT RECORD-TO-DATE (as of event start)
    desc.append("")
    desc.extend(ascii_rule(f"{opp_name} RECORD-TO-DATE".upper()))
    if g.start_utc and opp_id is not None:
        cutoff = g.start_utc
        opp_games_before = [
            gg for gg in all_games
            if gg.start_utc and gg.start_utc < cutoff
            and (gg.home_team_id == opp_id or gg.away_team_id == opp_id)
        ]
        rec = compute_record_to_date(opp_games_before, opp_id)
        desc.append(f"    {format_record_line(rec)}")
    else:
        desc.append("    Record-to-date: TBD")

    # OPPONENT GAMES-TO-DATE (all games start < event start, results if present)
    desc.append("")
    desc.extend(ascii_rule(f"{opp_name} GAMES-TO-DATE".upper()))
    if g.start_utc and opp_id is not None:
        cutoff = g.start_utc
        opp_games = [
            gg for gg in all_games
            if gg.start_utc and gg.start_utc < cutoff
            and (gg.home_team_id == opp_id or gg.away_team_id == opp_id)
        ]
        if opp_games:
            for gg in opp_games:
                desc.append(format_game_line_for_team(gg, opp_id, tz))
        else:
            desc.append("    (none)")
    else:
        desc.append("    (none)")

    # Check-in link
    desc.append("")
    desc.extend(ascii_rule("BTSH CHECK-IN / REGISTRATION"))
    desc.append(f"Check in for credit: {checkin_url}")

    return "\n".join(desc)


# ----------------------------
# ICS building
# ----------------------------

def build_vcalendar_header(name: str) -> List[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//btsh-ics//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(name)}",
        "X-WR-TIMEZONE:America/New_York",
    ]


def build_vevent(
    uid: str,
    dtstamp_utc: datetime,
    dtstart_local: Optional[datetime],
    dtend_local: Optional[datetime],
    tzid: str,
    summary: str,
    description: str,
    location: Optional[str],
) -> List[str]:
    lines = ["BEGIN:VEVENT"]
    lines.append(f"UID:{uid}")
    lines.append(f"DTSTAMP:{dtstamp_utc.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}")

    if dtstart_local is not None:
        lines.append(f"DTSTART;TZID={tzid}:{dt_to_ics(dtstart_local)}")
    if dtend_local is not None:
        lines.append(f"DTEND;TZID={tzid}:{dt_to_ics(dtend_local)}")

    lines.append(f"SUMMARY:{ics_escape(summary)}")
    if location:
        lines.append(f"LOCATION:{ics_escape(location)}")

    lines.append(f"DESCRIPTION:{ics_escape(description)}")
    lines.append("END:VEVENT")
    return lines


def build_team_calendar(
    team: TeamReg,
    season_name: str,
    season_year: int,
    tz: ZoneInfo,
    games: List[Game],
    div_map: Dict[int, Optional[str]],
    include_division_in_summary: bool,
    checkin_url: str,
) -> Tuple[str, List[str]]:
    team_games = [g for g in games if g.home_team_id == team.team_id or g.away_team_id == team.team_id]

    cal_name = f"BTSH {team.team_name} ({season_year})"
    lines = build_vcalendar_header(cal_name)

    dtstamp = now_utc()

    for g in team_games:
        # Convert times
        dtstart_local = g.start_utc.astimezone(tz) if g.start_utc else None
        dtend_local = g.end_utc.astimezone(tz) if g.end_utc else None

        _, opp_name, opp_id = team_side_and_opp(g, team.team_id)
        opp_div = div_map.get(opp_id) if opp_id else None

        summary = summary_for_team_game(
            g=g,
            team_id=team.team_id,
            tz=tz,
            team_div_short=team.division_short,
            opp_div_short=opp_div,
            include_division=include_division_in_summary,
        )

        desc = build_description_for_team_event(
            g=g,
            season_name=season_name,
            tz=tz,
            team_id=team.team_id,
            team_name=team.team_name,
            opp_name=opp_name,
            opp_id=opp_id,
            all_games=games,
            checkin_url=checkin_url,
        )

        # UID: stable for the same game+team. If games are rescheduled, UID remains stable if the API has an id.
        gid = g.raw.get("id")
        gid_str = str(gid) if gid is not None else (g.start_utc.isoformat() if g.start_utc else "TBD")
        uid = stable_uid(["btsh", f"season:{season_year}", f"team:{team.team_id}", f"game:{gid_str}"])

        lines.extend(
            build_vevent(
                uid=uid,
                dtstamp_utc=dtstamp,
                dtstart_local=dtstart_local,
                dtend_local=dtend_local,
                tzid=tz.key,
                summary=summary,
                description=desc,
                location=g.location,
            )
        )

    lines.append("END:VCALENDAR")

    filename = f"btsh-{safe_slug(team.team_name)}-season-{season_year}.ics"
    return filename, lines


def build_all_games_calendar(
    season_name: str,
    season_year: int,
    tz: ZoneInfo,
    games: List[Game],
    div_map: Dict[int, Optional[str]],
    include_division_in_summary: bool,
) -> Tuple[str, List[str]]:
    cal_name = f"BTSH All Games ({season_year})"
    lines = build_vcalendar_header(cal_name)

    dtstamp = now_utc()

    for g in games:
        dtstart_local = g.start_utc.astimezone(tz) if g.start_utc else None
        dtend_local = g.end_utc.astimezone(tz) if g.end_utc else None

        summary = summary_for_all_game(
            g=g,
            include_division=include_division_in_summary,
            div_map=div_map,
        )

        # Keep "all games" description simple
        desc: List[str] = []
        desc.extend(ascii_rule("GAME INFO"))
        desc.append(f"Season: {season_name}")
        desc.append(f"Stage: {g.stage_display or 'Unknown'}")
        desc.append(f"Status: {g.status or 'Unknown'}")
        if g.start_utc:
            dt_local = g.start_utc.astimezone(tz)
            desc.append(f"Start ({tz.key}): {dt_local:%Y-%m-%d %H:%M %Z}")
        else:
            desc.append(f"Start ({tz.key}): TBD")
        if g.location:
            desc.append(f"Location: {g.location}")

        gid = g.raw.get("id")
        gid_str = str(gid) if gid is not None else (g.start_utc.isoformat() if g.start_utc else "TBD")
        uid = stable_uid(["btsh", f"season:{season_year}", "all", f"game:{gid_str}"])

        lines.extend(
            build_vevent(
                uid=uid,
                dtstamp_utc=dtstamp,
                dtstart_local=dtstart_local,
                dtend_local=dtend_local,
                tzid=tz.key,
                summary=summary,
                description="\n".join(desc),
                location=g.location,
            )
        )

    lines.append("END:VCALENDAR")
    filename = f"btsh-all-games-season-{season_year}.ics"
    return filename, lines


# ----------------------------
# Main
# ----------------------------

def load_config(path: str) -> dict:
    if not os.path.exists(path):
        die(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        die("Config must be a YAML mapping/object.")
    return cfg


def main() -> None:
    cfg = load_config(DEFAULT_CONFIG_PATH)

    output_dir = cfg.get("output_dir", "docs")
    tz_name = cfg.get("default_timezone", "America/New_York")
    tz = ZoneInfo(tz_name)

    season_year = cfg.get("season_year")
    if not isinstance(season_year, int):
        die("Config must include season_year: <int> (e.g., 2026).")

    include_division_in_summary = bool(cfg.get("include_division_in_summary", True))
    checkin_url = cfg.get("checkin_url", "https://btsh.org/schedule")
    if not isinstance(checkin_url, str) or not checkin_url.strip():
        checkin_url = "https://btsh.org/schedule"

    print(f"Looking up BTSH season id for year={season_year} ...")
    season_id = btsh_get_season_id_for_year(season_year)
    season_name = f"{season_year} Season"

    print(f"Fetching team registrations for season_id={season_id} ...")
    teams = btsh_fetch_team_registrations(season_id)
    if not teams:
        die("No registered teams found for that season.")

    div_map: Dict[int, Optional[str]] = {t.team_id: t.division_short for t in teams}

    print(f"Fetching game days for season_id={season_id} ...")
    days = btsh_fetch_game_days(season_id)
    games = parse_games_from_days(days)

    # Only keep games involving registered teams OR placeholders (so placeholders show up)
    registered_ids = {t.team_id for t in teams}

    def keep_game(g: Game) -> bool:
        if is_placeholder(g):
            return True
        if g.home_team_id in registered_ids or g.away_team_id in registered_ids:
            return True
        # Some games might have missing team ids; keep as placeholder-ish
        if g.home_team_id is None or g.away_team_id is None:
            return True
        return False

    games = [g for g in games if keep_game(g)]

    print(f"Generating calendars into {output_dir}/ ...")
    written = 0

    # All games calendar
    all_name, all_lines = build_all_games_calendar(
        season_name=season_name,
        season_year=season_year,
        tz=tz,
        games=games,
        div_map=div_map,
        include_division_in_summary=include_division_in_summary,
    )
    write_ics(os.path.join(output_dir, all_name), all_lines)
    written += 1

    # Per-team calendars
    for t in sorted(teams, key=lambda x: x.team_name.lower()):
        fname, lines = build_team_calendar(
            team=t,
            season_name=season_name,
            season_year=season_year,
            tz=tz,
            games=games,
            div_map=div_map,
            include_division_in_summary=include_division_in_summary,
            checkin_url=checkin_url,
        )
        write_ics(os.path.join(output_dir, fname), lines)
        written += 1

    print(f"Done. Wrote {written} .ics files.")


if __name__ == "__main__":
    main()
