#!/usr/bin/env python3
"""
BTSH ICS Generator (season-year driven, pagination-safe)

Config:
- Prefer: season_year: 2025
- Legacy supported: season: 2   (season id)

Uses:
- Seasons: https://api.btsh.org/api/seasons/?
- Game days: https://api.btsh.org/api/game_days/?season=<season_id>
- Team regs: https://api.btsh.org/api/team-season-registrations/?season=<season_id>

Generates:
- one .ics per registered team (only teams registered that season)
- one "all games" .ics for that season

Description includes (per event):
- GAME INFO
- HEAD-TO-HEAD (prior matchups only)
- OPPONENT RECORD-TO-DATE (as of event start)
- OPPONENT GAMES-TO-DATE (all games with start < event start; include results if present)
- BTSH check-in link (for credit)

Notes:
- Cancelled games are INCLUDED and labeled.
- Placeholder/non-game items are INCLUDED and labeled when detectable.
- OT/SO detection is best-effort from status_display text.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def fmt_short_date_local(dt_local: datetime) -> str:
    return f"{dt_local:%b} {ordinal(dt_local.day)}"


def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_ics(dt_local: datetime) -> str:
    # floating local time; TZID is supplied on DTSTART/DTEND lines
    return dt_local.strftime("%Y%m%dT%H%M%S")


def stable_uid(parts: List[str]) -> str:
    raw = "||".join(parts)
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{h}@btsh-ics"


def ics_escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "\\n")
        .replace(",", "\\,")
        .replace(";", "\\;")
    )


def fold_ics_line(line: str, limit: int = 75) -> List[str]:
    if len(line) <= limit:
        return [line]
    out: List[str] = []
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
# Pagination / API helpers
# ----------------------------

def fetch_json(url: str, timeout_s: int = 30) -> Any:
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def unwrap_results(obj: Any) -> List[Any]:
    """
    Accept either:
      - a raw list
      - a paginated object with {"results": [...], "next": ...}
    """
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict) and isinstance(obj.get("results"), list):
        return obj["results"]
    return []


def fetch_all_pages(url: str, timeout_s: int = 30, hard_limit_pages: int = 25) -> List[Any]:
    """
    Fetch a list endpoint that may return either a raw list or a paginated dict.
    If paginated dict, follow `next` until exhausted.
    """
    first = fetch_json(url, timeout_s=timeout_s)
    if isinstance(first, list):
        return first

    if not isinstance(first, dict):
        die(f"Unexpected response type from {url}: {type(first)}")

    results: List[Any] = []
    results.extend(unwrap_results(first))

    next_url = first.get("next")
    pages = 1
    while next_url:
        pages += 1
        if pages > hard_limit_pages:
            die(f"Pagination exceeded hard limit ({hard_limit_pages}) at {url}")
        nxt = fetch_json(next_url, timeout_s=timeout_s)
        if isinstance(nxt, list):
            # unexpected, but handle it
            results.extend(nxt)
            break
        if not isinstance(nxt, dict):
            break
        results.extend(unwrap_results(nxt))
        next_url = nxt.get("next")

    return results


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
    start_utc: Optional[datetime]
    end_utc: Optional[datetime]
    location: Optional[str]
    status: Optional[str]
    status_display: Optional[str]
    stage_display: Optional[str]

    home_team_id: Optional[int]
    away_team_id: Optional[int]
    home_team_name: Optional[str]
    away_team_name: Optional[str]

    home_goals: Optional[int]
    away_goals: Optional[int]

    raw: Dict[str, Any]


# ----------------------------
# BTSH API (season lookup + data)
# ----------------------------

def btsh_get_season_id_for_year(year: int) -> int:
    seasons = fetch_all_pages("https://api.btsh.org/api/seasons/?")
    if not seasons:
        die("Seasons endpoint returned no items (or an unexpected shape).")

    matches = [s for s in seasons if isinstance(s, dict) and s.get("year") == year]
    if not matches:
        die(f"No season found with year={year}.")

    # If multiple, pick the one with latest start date (best guess)
    def sort_key(s: dict) -> Tuple[int, str]:
        start = s.get("start") or ""
        return (1 if s.get("is_current") else 0, start)

    matches.sort(key=sort_key, reverse=True)
    return int(matches[0]["id"])


def fetch_team_registrations(season_id: int) -> List[TeamReg]:
    url = f"https://api.btsh.org/api/team-season-registrations/?season={season_id}"
    regs = fetch_json(url)
    # This endpoint is paginated (dict with results), but we’ll handle both anyway:
    items = unwrap_results(regs) if isinstance(regs, dict) else (regs if isinstance(regs, list) else [])
    if not items:
        # try paginating just in case 'next' exists
        items = fetch_all_pages(url)

    out: List[TeamReg] = []
    for r in items:
        if not isinstance(r, dict):
            continue
        team = r.get("team") or {}
        div = r.get("division") or {}
        team_id = team.get("id")
        team_name = team.get("name")
        if not team_id or not team_name:
            continue
        out.append(
            TeamReg(
                team_id=int(team_id),
                team_name=str(team_name),
                division_name=(div.get("name") if isinstance(div, dict) else None),
                division_short=(div.get("short_name") if isinstance(div, dict) else None),
            )
        )
    return out


def parse_game_from_day_obj(day_obj: Dict[str, Any]) -> Optional[Game]:
    # Heuristics: day_obj may represent a "day" wrapper with games inside, OR a game itself.
    # We normalize by looking for common fields.
    raw = day_obj

    # Common fields:
    start = parse_iso_dt(raw.get("start") or raw.get("start_time") or raw.get("datetime") or raw.get("date_time"))
    end = parse_iso_dt(raw.get("end") or raw.get("end_time"))

    status = raw.get("status")
    status_display = raw.get("status_display") or raw.get("statusDisplay") or raw.get("display_status")
    stage_display = raw.get("stage_display") or raw.get("stage") or raw.get("stageDisplay")

    location = None
    # try common nesting patterns
    if isinstance(raw.get("location"), dict):
        location = raw["location"].get("name") or raw["location"].get("title")
    else:
        location = raw.get("location") or raw.get("rink") or raw.get("field") or raw.get("place")

    home = raw.get("home_team") or raw.get("homeTeam") or raw.get("team_home") or {}
    away = raw.get("away_team") or raw.get("awayTeam") or raw.get("team_away") or {}

    # sometimes just names/ids exist at top-level:
    home_team_id = (home.get("id") if isinstance(home, dict) else None) or raw.get("home_team_id")
    away_team_id = (away.get("id") if isinstance(away, dict) else None) or raw.get("away_team_id")
    home_team_name = (home.get("name") if isinstance(home, dict) else None) or raw.get("home_team_name")
    away_team_name = (away.get("name") if isinstance(away, dict) else None) or raw.get("away_team_name")

    # Scores may be nested or top-level
    home_goals = raw.get("home_score") or raw.get("homeScore") or raw.get("home_goals")
    away_goals = raw.get("away_score") or raw.get("awayScore") or raw.get("away_goals")

    def to_int(x: Any) -> Optional[int]:
        if x is None:
            return None
        try:
            return int(x)
        except Exception:
            return None

    return Game(
        start_utc=start,
        end_utc=end,
        location=str(location) if location else None,
        status=str(status) if status is not None else None,
        status_display=str(status_display) if status_display is not None else None,
        stage_display=str(stage_display) if stage_display is not None else None,
        home_team_id=to_int(home_team_id),
        away_team_id=to_int(away_team_id),
        home_team_name=str(home_team_name) if home_team_name else None,
        away_team_name=str(away_team_name) if away_team_name else None,
        home_goals=to_int(home_goals),
        away_goals=to_int(away_goals),
        raw=raw,
    )


def extract_games_from_game_days_payload(payload: Any) -> List[Game]:
    """
    The game_days endpoint in your earlier examples returned a LIST of day objects.
    Each day object may contain a list of games or a single game-like record.
    We try a few patterns safely.
    """
    days = payload if isinstance(payload, list) else unwrap_results(payload)
    out: List[Game] = []

    for day in days:
        if not isinstance(day, dict):
            continue

        # pattern A: day has "games" list
        games_list = day.get("games")
        if isinstance(games_list, list):
            for g in games_list:
                if isinstance(g, dict):
                    gg = parse_game_from_day_obj(g)
                    if gg:
                        out.append(gg)
            continue

        # pattern B: day has "matchups" list
        matchups = day.get("matchups")
        if isinstance(matchups, list):
            for g in matchups:
                if isinstance(g, dict):
                    gg = parse_game_from_day_obj(g)
                    if gg:
                        out.append(gg)
            continue

        # pattern C: day itself is game-like
        gg = parse_game_from_day_obj(day)
        if gg:
            out.append(gg)

    return out


def fetch_game_days(season_id: int, api_url_template: str) -> List[Game]:
    url = api_url_template.format(season=season_id, season_id=season_id)
    payload = fetch_json(url)
    games = extract_games_from_game_days_payload(payload)
    if not games:
        # if API ever becomes paginated, this helps
        payload2 = fetch_all_pages(url)
        games = extract_games_from_game_days_payload(payload2)
    return games


# ----------------------------
# Game logic / formatting
# ----------------------------

def is_cancelled(g: Game) -> bool:
    s = (g.status or "").lower()
    sd = (g.status_display or "").lower()
    return "cancel" in s or "cancel" in sd


def is_placeholder_or_league_day(g: Game) -> bool:
    # Heuristic: missing teams or "tbd" / "-" names
    hn = (g.home_team_name or "").strip()
    an = (g.away_team_name or "").strip()
    joined = f"{hn} {an} {(g.status_display or '')}".lower()
    if not hn or not an:
        return True
    if hn in {"-", "tbd"} or an in {"-", "tbd"}:
        return True
    if "placeholder" in joined or "rain" in joined or "make up" in joined or "make-up" in joined or "playoff" in joined:
        return True
    return False


def winner_label(g: Game, team_id: int) -> Optional[str]:
    """
    Returns something like:
      "W 10-5"
      "L 2-6"
      "W(OT) 4-3"
      "L(SO) 3-4"
    or None if result unknown.
    """
    if g.home_goals is None or g.away_goals is None:
        return None
    if g.home_team_id is None or g.away_team_id is None:
        return None

    # Determine OT/SO tag from status_display
    tag = ""
    sd = (g.status_display or "").upper()
    if "OT" in sd:
        tag = "(OT)"
    elif "SO" in sd or "S/O" in sd:
        tag = "(SO)"

    # Figure team’s score/opponent score
    if team_id == g.home_team_id:
        my, opp = g.home_goals, g.away_goals
    elif team_id == g.away_team_id:
        my, opp = g.away_goals, g.home_goals
    else:
        return None

    if my > opp:
        return f"W{tag} {my}-{opp}"
    if my < opp:
        return f"L{tag} {my}-{opp}"
    return f"T{tag} {my}-{opp}"


def opponent_id_and_name(my_team_id: int, g: Game) -> Tuple[Optional[int], Optional[str], str]:
    """
    Returns (opp_id, opp_name, vs_at_token) where vs_at_token is "vs" if home, "@"
    """
    if g.home_team_id == my_team_id:
        return g.away_team_id, g.away_team_name, "vs"
    if g.away_team_id == my_team_id:
        return g.home_team_id, g.home_team_name, "@"
    return None, None, "vs"


def team_game_label_for_opponent_view(opp_team_id: int, g: Game, tz: ZoneInfo) -> Optional[str]:
    """
    For opponent games list:
      "Jan 22nd vs Michigan (L 0-8)"
      "Feb 6th @ Blizzard"
    """
    if not g.start_utc:
        return None
    dt_local = g.start_utc.astimezone(tz)
    date_part = fmt_short_date_local(dt_local)

    # Determine opponent's perspective (vs/@ + other team name)
    if g.home_team_id == opp_team_id:
        token = "vs"
        other = g.away_team_name or "TBD"
    elif g.away_team_id == opp_team_id:
        token = "@"
        other = g.home_team_name or "TBD"
    else:
        # not actually their game
        token = "vs"
        other = "TBD"

    # Result if known
    res = winner_label(g, opp_team_id)
    if res:
        return f"    {date_part} {token} {other} ({res})"
    return f"    {date_part} {token} {other}"


def calc_record_to_date(team_id: int, games: List[Game], cutoff_start: datetime) -> Tuple[int, int, int, int, int]:
    """
    Returns (W, L, T, OTW, SOW) best-effort.
    We classify OT/SO wins if status_display includes OT/SO.
    Losses include OT/SO losses as L (you can expand later if you want split).
    Only games with start < cutoff_start and with scores count.
    """
    w = l = t = otw = sow = 0
    for g in games:
        if not g.start_utc or g.start_utc >= cutoff_start:
            continue
        if g.home_goals is None or g.away_goals is None:
            continue
        if g.home_team_id != team_id and g.away_team_id != team_id:
            continue

        res = winner_label(g, team_id)
        if not res:
            continue

        sd = (g.status_display or "").upper()
        is_ot = "OT" in sd
        is_so = ("SO" in sd) or ("S/O" in sd)

        if res.startswith("W"):
            w += 1
            if is_so:
                sow += 1
            elif is_ot:
                otw += 1
        elif res.startswith("L"):
            l += 1
        else:
            t += 1
    return w, l, t, otw, sow


# ----------------------------
# ICS building
# ----------------------------

def ics_calendar_header(name: str) -> List[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//btsh-ics//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(name)}",
    ]


def ics_calendar_footer() -> List[str]:
    return ["END:VCALENDAR"]


def build_event(
    *,
    tz: ZoneInfo,
    team_name: str,
    team_id: Optional[int],
    team_div_short: Optional[str],
    season_year: int,
    game: Game,
    all_games_for_team: List[Game],
    opponent_recent_limit: int,
    include_checkin_link: bool,
) -> List[str]:
    if not game.start_utc:
        return []

    start_local = game.start_utc.astimezone(tz)
    end_local = (game.end_utc.astimezone(tz) if game.end_utc else start_local.replace(hour=start_local.hour + 1))

    # Title: include division in name (optional; only for team calendars where we know it)
    home = game.home_team_name or "TBD"
    away = game.away_team_name or "TBD"
    vs_title = f"{away} @ {home}"
    if team_div_short:
        summary = f"[D{team_div_short}] {vs_title}"
    else:
        summary = vs_title

    # Status line
    status_line = (game.status_display or game.status or "scheduled").strip()
    if is_cancelled(game):
        status_line = f"{status_line} (CANCELLED)"

    # Description blocks
    desc: List[str] = []
    desc.extend(ascii_rule("GAME INFO"))
    desc.append(f"Season: {season_year}")
    if game.stage_display:
        desc.append(f"Stage: {game.stage_display}")
    desc.append(f"Status: {status_line}")
    desc.append(f"Start ({tz.key}): {start_local:%Y-%m-%d %H:%M %Z}")
    if game.location:
        desc.append(f"Location: {game.location}")

    # If this is a placeholder / league day with TBD teams, keep it simple
    if team_id is None or is_placeholder_or_league_day(game):
        if include_checkin_link:
            desc.append("")
            desc.append("Check-in / registration:")
            desc.append("https://btsh.org/")
        description = "\n".join(desc)

        uid = stable_uid(["btsh", str(season_year), "all", summary, start_local.isoformat()])
        return [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{now_utc().strftime('%Y%m%dT%H%M%SZ')}",
            f"SUMMARY:{ics_escape(summary)}",
            f"DTSTART;TZID={tz.key}:{dt_to_ics(start_local)}",
            f"DTEND;TZID={tz.key}:{dt_to_ics(end_local)}",
            f"DESCRIPTION:{ics_escape(description)}",
            "END:VEVENT",
        ]

    # Team context: identify opponent for this team
    opp_id, opp_name, token = opponent_id_and_name(team_id, game)
    opp_name = opp_name or "TBD"

    # HEAD-TO-HEAD (prior matchups only)
    prior_h2h: List[Game] = []
    for g in all_games_for_team:
        if not g.start_utc or g.start_utc >= game.start_utc:
            continue
        ids = {g.home_team_id, g.away_team_id}
        if team_id in ids and opp_id in ids:
            prior_h2h.append(g)
    prior_h2h.sort(key=lambda g: g.start_utc or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    desc.append("")
    desc.extend(ascii_rule(f"HEAD-TO-HEAD vs {opp_name}"))
    if prior_h2h:
        for g in prior_h2h[:opponent_recent_limit]:
            dt_local = g.start_utc.astimezone(tz) if g.start_utc else start_local
            date_part = fmt_short_date_local(dt_local)
            # perspective for *your* team
            if g.home_team_id == team_id:
                tok = "vs"
            else:
                tok = "@"
            res = winner_label(g, team_id)
            if res:
                desc.append(f"    {date_part} {tok} {opp_name} ({res})")
            else:
                desc.append(f"    {date_part} {tok} {opp_name}")
    else:
        desc.append("    (no prior matchups)")

    # Opponent record-to-date (as of event start)
    opp_games = [g for g in all_games_for_team if (g.home_team_id == opp_id or g.away_team_id == opp_id)]
    w, l, t, otw, sow = calc_record_to_date(opp_id or -1, opp_games, cutoff_start=game.start_utc)
    desc.append("")
    desc.extend(ascii_rule(f"{opp_name} RECORD-TO-DATE"))
    desc.append(f"    W-L-T: {w}-{l}-{t}   (OTW: {otw}, SOW: {sow})")

    # Opponent games-to-date (all games with start < event start)
    opp_prior = [g for g in opp_games if g.start_utc and g.start_utc < game.start_utc]
    opp_prior.sort(key=lambda g: g.start_utc or datetime.max.replace(tzinfo=timezone.utc))

    desc.append("")
    desc.extend(ascii_rule(f"{opp_name} GAMES-TO-DATE"))
    shown = 0
    for g in opp_prior:
        line = team_game_label_for_opponent_view(opp_id or -1, g, tz)
        if not line:
            continue
        desc.append(line)
        shown += 1
        if opponent_recent_limit and shown >= opponent_recent_limit:
            break
    if shown == 0:
        desc.append("    (no prior games)")

    if include_checkin_link:
        desc.append("")
        desc.append("Check-in / registration:")
        desc.append("https://btsh.org/")

    description = "\n".join(desc)

    # UID should be stable for this team + game start + matchup
    uid = stable_uid([
        "btsh",
        str(season_year),
        str(team_id),
        str(game.home_team_id),
        str(game.away_team_id),
        start_local.isoformat(),
    ])

    return [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{now_utc().strftime('%Y%m%dT%H%M%SZ')}",
        f"SUMMARY:{ics_escape(summary)}",
        f"DTSTART;TZID={tz.key}:{dt_to_ics(start_local)}",
        f"DTEND;TZID={tz.key}:{dt_to_ics(end_local)}",
        f"DESCRIPTION:{ics_escape(description)}",
        "END:VEVENT",
    ]


# ----------------------------
# Main
# ----------------------------

def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        die(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        die("Config must be a YAML mapping/object.")
    return cfg


def main() -> None:
    cfg = load_config(DEFAULT_CONFIG_PATH)

    out_dir = str(cfg.get("output_dir") or "docs")
    tz_name = str(cfg.get("default_timezone") or "America/New_York")
    tz = ZoneInfo(tz_name)

    # New preferred: season_year
    season_year = cfg.get("season_year") or cfg.get("year")
    # Legacy: season (id)
    season_id_legacy = cfg.get("season")

    api_url_template = str(cfg.get("api_url") or "https://api.btsh.org/api/game_days/?season={season}")
    opponent_recent_limit = int(cfg.get("opponent_recent_limit") or 10)

    include_checkin_link = True

    if season_year is not None:
        try:
            season_year_int = int(season_year)
        except Exception:
            die("season_year must be an integer (e.g., 2025).")

        print(f"Looking up BTSH season id for year={season_year_int} ...")
        season_id = btsh_get_season_id_for_year(season_year_int)
        season_year_final = season_year_int
    elif season_id_legacy is not None:
        try:
            season_id = int(season_id_legacy)
        except Exception:
            die("season must be an integer season id.")
        # best-effort year label if not provided
        season_year_final = int(cfg.get("season_label_year") or 0) or season_id
        print(f"Using legacy season id={season_id} (consider switching to season_year).")
    else:
        die("Config must include either season_year (preferred) or season (legacy season id).")

    regs = fetch_team_registrations(season_id)
    if not regs:
        die(f"No team registrations found for season_id={season_id}.")

    games = fetch_game_days(season_id, api_url_template)
    if not games:
        die(f"No games returned from game_days endpoint for season_id={season_id}.")

    # Build a canonical list per team_id
    regs_by_id: Dict[int, TeamReg] = {r.team_id: r for r in regs}

    # ALL-GAMES calendar (only games that involve registered teams OR placeholders)
    all_cal_lines: List[str] = []
    all_cal_lines.extend(ics_calendar_header(f"BTSH All Games ({season_year_final})"))

    # For per-team calendars, we need "all games for that team" (to compute h2h/opponent)
    games_by_team: Dict[int, List[Game]] = {tid: [] for tid in regs_by_id.keys()}

    for g in games:
        ids = {g.home_team_id, g.away_team_id}
        matched_team_ids = [tid for tid in regs_by_id.keys() if tid in ids]

        # add to team buckets
        for tid in matched_team_ids:
            games_by_team[tid].append(g)

        # add to all-games calendar if it touches registered teams or is placeholder/day
        if matched_team_ids or is_placeholder_or_league_day(g):
            ev = build_event(
                tz=tz,
                team_name="ALL",
                team_id=None,  # no team-specific context
                team_div_short=None,
                season_year=season_year_final,
                game=g,
                all_games_for_team=[],
                opponent_recent_limit=opponent_recent_limit,
                include_checkin_link=include_checkin_link,
            )
            all_cal_lines.extend(ev)

    all_cal_lines.extend(ics_calendar_footer())
    all_path = os.path.join(out_dir, f"btsh-all-games-season-{season_year_final}.ics")
    write_ics(all_path, all_cal_lines)
    print(f"Wrote {all_path}")

    # Per-team calendars
    for tid, reg in regs_by_id.items():
        cal_lines: List[str] = []
        cal_lines.extend(ics_calendar_header(f"BTSH {reg.team_name} ({season_year_final})"))

        team_games = games_by_team.get(tid, [])
        # For richer context, keep all games sorted
        team_games_sorted = sorted(
            team_games,
            key=lambda g: g.start_utc or datetime.max.replace(tzinfo=timezone.utc),
        )

        for g in team_games_sorted:
            ev = build_event(
                tz=tz,
                team_name=reg.team_name,
                team_id=tid,
                team_div_short=reg.division_short,
                season_year=season_year_final,
                game=g,
                all_games_for_team=team_games_sorted,
                opponent_recent_limit=opponent_recent_limit,
                include_checkin_link=include_checkin_link,
            )
            cal_lines.extend(ev)

        cal_lines.extend(ics_calendar_footer())
        filename = f"btsh-{safe_slug(reg.team_name)}-season-{season_year_final}.ics"
        path = os.path.join(out_dir, filename)
        write_ics(path, cal_lines)
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()