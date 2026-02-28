#!/usr/bin/env python3
"""
BTSH ICS generator

Outputs:
- One calendar per registered team for the configured season_year
- One "all games" calendar for the season

Features:
- Opponent record-to-date (from team-season-registrations)
- Head-to-head prior matchups only
- Opponent games-to-date (games before the event start time)
- Division in event title
- Best-effort Reg/OT/SO result labeling
- Robust handling of paginated API responses ({"results":[...]}) vs raw lists
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml

# ----------------------------
# Small helpers
# ----------------------------

def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)

def load_yaml(path: str) -> dict:
    if not os.path.exists(path):
        die(f"Missing config file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def fetch_json(url: str, timeout: int = 30) -> Any:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def as_list(maybe_list_or_page: Any) -> List[Any]:
    """Accepts a raw list OR a paginated dict with 'results'."""
    if isinstance(maybe_list_or_page, list):
        return maybe_list_or_page
    if isinstance(maybe_list_or_page, dict):
        if "results" in maybe_list_or_page and isinstance(maybe_list_or_page["results"], list):
            return maybe_list_or_page["results"]
    # last resort: if it's dict but looks like a single object, wrap it
    if isinstance(maybe_list_or_page, dict):
        return [maybe_list_or_page]
    return []

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "team"

def stable_uid(prefix: str, raw: str) -> str:
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{h}@btsh-ics"

def ascii_rule(title: str) -> List[str]:
    line = "-" * 40
    return [line, title, line]

def ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"

def fmt_short_date_local(dt_local: datetime) -> str:
    # Example: "Feb 20th"
    return f"{dt_local.strftime('%b')} {ordinal(dt_local.day)}"

def ensure_dt(dt: Any) -> Optional[datetime]:
    """Parse an ISO-ish datetime string into aware datetime."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    if not isinstance(dt, str):
        return None
    s = dt.strip()
    if not s:
        return None
    # allow "Z"
    s = s.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None

def to_local(dt_aware: datetime, tz_name: str) -> datetime:
    # Python 3.11+ supports zoneinfo
    from zoneinfo import ZoneInfo
    return dt_aware.astimezone(ZoneInfo(tz_name))

def ics_escape(text: str) -> str:
    # RFC5545 escaping for TEXT
    text = text.replace("\\", "\\\\")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", "\\n")
    text = text.replace(",", "\\,").replace(";", "\\;")
    return text

def fold_ics_line(line: str, limit: int = 75) -> List[str]:
    # Soft wrap per RFC (octets-ish; we do chars, good enough for ASCII)
    if len(line) <= limit:
        return [line]
    out = []
    while len(line) > limit:
        out.append(line[:limit])
        line = " " + line[limit:]
    out.append(line)
    return out

# ----------------------------
# Domain models
# ----------------------------

@dataclass(frozen=True)
class TeamReg:
    team_id: int
    team_name: str
    division_name: str
    division_short: str
    record: str
    regulation_wins: int
    regulation_losses: int
    overtime_wins: int
    overtime_losses: int
    shootout_wins: int
    shootout_losses: int
    ties: int

    def record_line(self) -> str:
        # Human compact: "Record: 15-3-0 (Reg 12-2, OT 1-0, SO 2-1)"
        parts = []
        parts.append(f"Record: {self.record}")
        sub = []
        sub.append(f"Reg {self.regulation_wins}-{self.regulation_losses}")
        sub.append(f"OT {self.overtime_wins}-{self.overtime_losses}")
        sub.append(f"SO {self.shootout_wins}-{self.shootout_losses}")
        if self.ties:
            sub.append(f"T {self.ties}")
        return f"{parts[0]} ({', '.join(sub)})"

@dataclass
class Game:
    game_id: str
    start_utc: Optional[datetime]
    end_utc: Optional[datetime]
    location: str
    status: str

    home_id: Optional[int]
    home_name: str
    away_id: Optional[int]
    away_name: str

    home_score: Optional[int]
    away_score: Optional[int]

    # best-effort flags
    went_ot: bool
    went_so: bool
    cancelled: bool
    placeholder: bool
    note: str

    def has_teams(self) -> bool:
        return bool(self.home_name and self.away_name)

    def is_tbd(self) -> bool:
        # placeholder / TBD team names / missing ids
        names = (self.home_name.strip().lower(), self.away_name.strip().lower())
        return self.placeholder or any(n in ("tbd", "-", "—", "bye") for n in names) or not self.has_teams()

    def involves_team_id(self, team_id: int) -> bool:
        return self.home_id == team_id or self.away_id == team_id

    def opponent_for_team(self, team_id: int) -> Tuple[Optional[int], str]:
        if self.home_id == team_id:
            return self.away_id, self.away_name
        if self.away_id == team_id:
            return self.home_id, self.home_name
        return None, ""

    def is_finalish(self) -> bool:
        # common status values: final, completed, played, etc.
        s = (self.status or "").strip().lower()
        if self.cancelled:
            return False
        return s in ("final", "completed", "complete", "played", "done") or (self.home_score is not None and self.away_score is not None)

    def result_tag(self) -> str:
        # "", " (OT)", " (SO)"
        if self.went_so:
            return " (SO)"
        if self.went_ot:
            return " (OT)"
        return ""

    def winner_for(self, team_id: int) -> Optional[str]:
        if not self.is_finalish():
            return None
        if self.home_score is None or self.away_score is None:
            return None
        if self.home_id == team_id:
            if self.home_score > self.away_score:
                return "W"
            if self.home_score < self.away_score:
                return "L"
            return "T"
        if self.away_id == team_id:
            if self.away_score > self.home_score:
                return "W"
            if self.away_score < self.home_score:
                return "L"
            return "T"
        return None

    def score_str_for(self, team_id: int) -> str:
        if self.home_score is None or self.away_score is None:
            return ""
        if self.home_id == team_id:
            return f"{self.home_score}-{self.away_score}"
        if self.away_id == team_id:
            return f"{self.away_score}-{self.home_score}"
        return f"{self.home_score}-{self.away_score}"

# ----------------------------
# Parsing BTSH API (best-effort, schema-tolerant)
# ----------------------------

def pick(obj: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return default

def parse_team_obj(t: Any) -> Tuple[Optional[int], str]:
    if t is None:
        return None, ""
    if isinstance(t, dict):
        tid = pick(t, ["id", "team_id"])
        name = pick(t, ["name", "team_name", "short_name"], "") or ""
        try:
            tid_i = int(tid) if tid is not None else None
        except Exception:
            tid_i = None
        return tid_i, str(name)
    # sometimes just a name string
    if isinstance(t, str):
        return None, t
    return None, ""

def parse_game_obj(g: Dict[str, Any]) -> Game:
    # time
    start_raw = pick(g, ["start", "start_time", "datetime", "game_time", "time", "starts_at"])
    end_raw = pick(g, ["end", "end_time", "ends_at"])
    start_dt = ensure_dt(start_raw)
    end_dt = ensure_dt(end_raw)
    if start_dt and not end_dt:
        end_dt = start_dt + timedelta(hours=1)  # default

    # status / cancellation
    status = str(pick(g, ["status", "state"], "scheduled") or "scheduled")
    cancelled = bool(pick(g, ["cancelled", "is_cancelled"], False)) or status.strip().lower() in ("cancelled", "canceled")

    # teams
    home = pick(g, ["home_team", "home", "team_home", "team1", "team_1"])
    away = pick(g, ["away_team", "away", "team_away", "team2", "team_2"])
    home_id, home_name = parse_team_obj(home)
    away_id, away_name = parse_team_obj(away)

    # some APIs give flat fields
    if not home_name:
        home_name = str(pick(g, ["home_team_name", "home_name"], "") or "")
    if not away_name:
        away_name = str(pick(g, ["away_team_name", "away_name"], "") or "")
    if home_id is None:
        hid = pick(g, ["home_team_id", "home_id"])
        try:
            home_id = int(hid) if hid is not None else None
        except Exception:
            home_id = None
    if away_id is None:
        aid = pick(g, ["away_team_id", "away_id"])
        try:
            away_id = int(aid) if aid is not None else None
        except Exception:
            away_id = None

    # scores
    hs = pick(g, ["home_score", "score_home", "home_goals", "team1_score"])
    aws = pick(g, ["away_score", "score_away", "away_goals", "team2_score"])
    try:
        home_score = int(hs) if hs is not None and hs != "" else None
    except Exception:
        home_score = None
    try:
        away_score = int(aws) if aws is not None and aws != "" else None
    except Exception:
        away_score = None

    # OT/SO flags: can be bools or strings
    went_ot = bool(pick(g, ["went_overtime", "overtime", "is_overtime"], False))
    went_so = bool(pick(g, ["went_shootout", "shootout", "is_shootout"], False))

    # sometimes a "result_type" exists
    rt = str(pick(g, ["result_type", "finish_type"], "") or "").strip().lower()
    if rt in ("ot", "overtime"):
        went_ot = True
    if rt in ("so", "shootout"):
        went_so = True

    placeholder = bool(pick(g, ["placeholder", "is_placeholder"], False))
    note = str(pick(g, ["note", "description", "details"], "") or "")

    location = str(pick(g, ["location", "rink", "field", "venue"], "") or "")

    gid = pick(g, ["id", "game_id"])
    gid_s = str(gid) if gid is not None else stable_uid("btsh-game", f"{home_name}|{away_name}|{start_raw}")

    return Game(
        game_id=gid_s,
        start_utc=start_dt,
        end_utc=end_dt,
        location=location,
        status=status,
        home_id=home_id,
        home_name=home_name.strip() or "TBD",
        away_id=away_id,
        away_name=away_name.strip() or "TBD",
        home_score=home_score,
        away_score=away_score,
        went_ot=went_ot,
        went_so=went_so,
        cancelled=cancelled,
        placeholder=placeholder,
        note=note.strip(),
    )

def extract_games_from_game_days(game_days_payload: Any) -> Tuple[List[Game], List[dict]]:
    """
    Returns:
      - games: flattened list of Game
      - league_days: non-game "days" / notes (kept as dict, later converted to all-day VEVENTs)
    """
    days = as_list(game_days_payload)
    games: List[Game] = []
    league_days: List[dict] = []

    for d in days:
        if not isinstance(d, dict):
            continue

        # league-wide days / placeholders could be encoded at the day level
        day_type = str(pick(d, ["type", "day_type", "category"], "") or "").strip().lower()
        if day_type and day_type not in ("game", "games"):
            league_days.append(d)

        # games list might be under a few keys
        gs = pick(d, ["games", "game_scores", "matches", "events"], [])
        for g in as_list(gs):
            if isinstance(g, dict):
                games.append(parse_game_obj(g))

    return games, league_days

# ----------------------------
# ICS building
# ----------------------------

def build_vtimezone(tz_name: str) -> List[str]:
    # Many clients accept TZID without VTIMEZONE; but adding improves compatibility.
    # Minimal VTIMEZONE is hard to do perfectly (DST rules), so we omit it intentionally.
    # We still output DTSTART;TZID=<tz>.
    return []

def dt_local_ics(dt_local: datetime, tz_name: str) -> str:
    # DTSTART;TZID=America/New_York:20250326T203000
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

    if dtstart_local and dtend_local:
        lines += [f"DTSTART;TZID={tz_name}:{dt_local_ics(dtstart_local, tz_name)}"]
        lines += [f"DTEND;TZID={tz_name}:{dt_local_ics(dtend_local, tz_name)}"]

    if location:
        lines += [f"LOCATION:{ics_escape(location)}"]

    if url:
        lines += [f"URL:{ics_escape(url)}"]

    desc = "\n".join(description_lines).strip()
    lines += [f"DESCRIPTION:{ics_escape(desc)}"]

    lines += ["END:VEVENT"]
    # fold
    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded

def ics_allday_event(uid: str, summary: str, day_local: datetime, description_lines: List[str]) -> List[str]:
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
        "END:VEVENT",
    ]

    folded: List[str] = []
    for ln in lines:
        folded.extend(fold_ics_line(ln))
    return folded

def calendar_header(cal_name: str) -> List[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//btsh-ics//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{ics_escape(cal_name)}",
    ]

def calendar_footer() -> List[str]:
    return ["END:VCALENDAR"]

# ----------------------------
# Description formatting
# ----------------------------

def format_game_line_for_opponent_view(g: Game, opp_team_id: int, tz_name: str) -> str:
    """
    Opponent-centric line:
        "Jan 22nd vs Michigan (L 0-8)"
        "Feb 6th @ Blizzard"
    """
    if not g.start_utc:
        date_part = "TBD"
    else:
        date_part = fmt_short_date_local(to_local(g.start_utc, tz_name))

    # figure out opponent's opponent
    if g.home_id == opp_team_id:
        vs_at = "vs"
        other = g.away_name
    elif g.away_id == opp_team_id:
        vs_at = "@"
        other = g.home_name
    else:
        # fallback
        vs_at = "vs"
        other = g.away_name or g.home_name or "TBD"

    suffix = ""
    if g.is_finalish() and g.home_score is not None and g.away_score is not None:
        wlt = g.winner_for(opp_team_id) or ""
        score = g.score_str_for(opp_team_id)
        if wlt == "T":
            suffix = f" (T {score}){g.result_tag()}"
        else:
            suffix = f" ({wlt} {score}){g.result_tag()}"
    return f"    {date_part} {vs_at} {other}{suffix}".rstrip()

def format_head_to_head_line(g: Game, my_id: int, opp_name: str, tz_name: str) -> str:
    # "Feb 20th vs Slainte Ice Dragons" (always prior matchups only)
    if not g.start_utc:
        date_part = "TBD"
    else:
        date_part = fmt_short_date_local(to_local(g.start_utc, tz_name))
    # from my perspective, show vs/@ opponent
    if g.home_id == my_id:
        vs_at = "vs"
    elif g.away_id == my_id:
        vs_at = "@"
    else:
        vs_at = "vs"
    suffix = ""
    if g.is_finalish() and g.home_score is not None and g.away_score is not None:
        wlt = g.winner_for(my_id) or ""
        score = g.score_str_for(my_id)
        if wlt == "T":
            suffix = f" (T {score}){g.result_tag()}"
        else:
            suffix = f" ({wlt} {score}){g.result_tag()}"
    return f"    {date_part} {vs_at} {opp_name}{suffix}".rstrip()

# ----------------------------
# Main build
# ----------------------------

def find_season_id_for_year(seasons_payload: Any, year: int) -> int:
    seasons = as_list(seasons_payload)
    for s in seasons:
        if not isinstance(s, dict):
            continue
        y = pick(s, ["year"])
        sid = pick(s, ["id"])
        try:
            if int(y) == int(year):
                return int(sid)
        except Exception:
            continue
    die(f"Could not find season id for year={year} in seasons response")

def main() -> None:
    cfg = load_yaml("config.yml")

    out_dir = str(cfg.get("output_dir", "docs"))
    tz_name = str(cfg.get("default_timezone", "America/New_York"))

    season_year = cfg.get("season_year")
    if season_year is None:
        die("config.yml must include season_year (e.g. 2025)")
    try:
        season_year = int(season_year)
    except Exception:
        die("season_year must be an integer")

    seasons_url = str(cfg.get("seasons_api_url", "https://api.btsh.org/api/seasons/"))
    reg_url_tpl = str(cfg.get("registrations_api_url", "https://api.btsh.org/api/team-season-registrations/?season={season_id}"))
    gd_url_tpl = str(cfg.get("game_days_api_url", "https://api.btsh.org/api/game_days/?season={season_id}"))

    opp_limit = int(cfg.get("opponent_recent_limit", 10))
    include_league_wide_days = bool(cfg.get("include_league_wide_days", True))
    include_tbd_on_all = bool(cfg.get("include_tbd_games_on_all_calendars", True))
    checkin_url = str(cfg.get("checkin_url", "https://btsh.org"))

    os.makedirs(out_dir, exist_ok=True)

    print(f"Looking up BTSH season id for year={season_year} ...")
    seasons_payload = fetch_json(seasons_url)
    season_id = find_season_id_for_year(seasons_payload, season_year)
    print(f"Using season_id={season_id}")

    print("Fetching team registrations ...")
    regs_payload = fetch_json(reg_url_tpl.format(season_id=season_id))
    regs_list = as_list(regs_payload)

    team_regs: Dict[int, TeamReg] = {}
    for r in regs_list:
        if not isinstance(r, dict):
            continue
        team = r.get("team") or {}
        div = r.get("division") or {}
        try:
            tid = int(pick(team, ["id"]))
        except Exception:
            continue
        team_name = str(pick(team, ["name"], "") or "").strip()
        div_name = str(pick(div, ["name"], "") or "").strip() or "Division"
        div_short = str(pick(div, ["short_name"], "") or "").strip() or div_name

        rec = str(pick(r, ["record"], "") or "").strip() or f"{pick(r, ['wins'], 0)}-{pick(r, ['losses'], 0)}-{pick(r, ['ties'], 0)}"
        def geti(k: str) -> int:
            try:
                return int(pick(r, [k], 0) or 0)
            except Exception:
                return 0

        team_regs[tid] = TeamReg(
            team_id=tid,
            team_name=team_name or f"Team {tid}",
            division_name=div_name,
            division_short=div_short,
            record=rec,
            regulation_wins=geti("regulation_wins"),
            regulation_losses=geti("regulation_losses"),
            overtime_wins=geti("overtime_wins"),
            overtime_losses=geti("overtime_losses"),
            shootout_wins=geti("shootout_wins"),
            shootout_losses=geti("shootout_losses"),
            ties=geti("ties"),
        )

    if not team_regs:
        die("No team registrations found; cannot build team calendars.")

    print(f"Registered teams: {len(team_regs)}")

    print("Fetching game days ...")
    game_days_payload = fetch_json(gd_url_tpl.format(season_id=season_id))
    games, league_days = extract_games_from_game_days(game_days_payload)

    # normalize games with sortable start
    games = [g for g in games if g.start_utc is not None] + [g for g in games if g.start_utc is None]

    # Index games by team for quick lookup
    games_by_team: Dict[int, List[Game]] = {tid: [] for tid in team_regs.keys()}
    all_games: List[Game] = []

    for g in games:
        all_games.append(g)
        # only assign to registered teams
        if g.home_id in games_by_team:
            games_by_team[g.home_id].append(g)
        if g.away_id in games_by_team:
            games_by_team[g.away_id].append(g)

    # Helper for filtering "prior" games relative to an event start
    def is_before_event(g: Game, event_start: datetime) -> bool:
        if not g.start_utc:
            return False
        return g.start_utc < event_start

    # Sort each list
    def sort_key(g: Game):
        return (g.start_utc or datetime.max.replace(tzinfo=timezone.utc), g.game_id)

    for tid in games_by_team:
        games_by_team[tid].sort(key=sort_key)
    all_games.sort(key=sort_key)

    # ----------------------------
    # Build per-team calendars
    # ----------------------------

    def build_team_event_lines(team_id: int, g: Game) -> List[str]:
        reg = team_regs[team_id]
        my_name = reg.team_name
        div_short = reg.division_short

        # determine opponent
        opp_id, opp_name = g.opponent_for_team(team_id)
        opp_reg = team_regs.get(opp_id) if opp_id is not None else None

        # title
        if g.cancelled:
            summary = f"[D{div_short}] CANCELLED: {my_name} vs {opp_name}"
        else:
            # keep home/away semantics
            if g.home_id == team_id:
                summary = f"[D{div_short}] {my_name} vs {opp_name}"
            else:
                summary = f"[D{div_short}] {my_name} @ {opp_name}"

        # times
        dtstart_local = to_local(g.start_utc, tz_name) if g.start_utc else None
        dtend_local = to_local(g.end_utc, tz_name) if g.end_utc else (dtstart_local + timedelta(hours=1) if dtstart_local else None)

        # description
        desc: List[str] = []
        desc.extend(ascii_rule("GAME INFO"))
        desc.append(f"Season: {season_year}")
        desc.append(f"Division: {reg.division_name}")
        desc.append(f"Status: {g.status}{' (cancelled)' if g.cancelled else ''}")

        if dtstart_local:
            desc.append(f"Start ({tz_name}): {dtstart_local.strftime('%Y-%m-%d %H:%M %Z')}")
        if g.location:
            desc.append(f"Location: {g.location}")
        if g.note:
            desc.append(f"Note: {g.note}")

        desc.append("")
        if opp_reg:
            desc.extend(ascii_rule(f"OPPONENT RECORD-TO-DATE: {opp_reg.team_name.upper()}"))
            desc.append(opp_reg.record_line())
            desc.append("")

        # head-to-head (prior only)
        if opp_id is not None and g.start_utc:
            prior_h2h = []
            for pg in games_by_team[team_id]:
                if not pg.start_utc or not is_before_event(pg, g.start_utc):
                    continue
                # match same opponent by id when possible, else by name
                if opp_id is not None and (pg.home_id == opp_id or pg.away_id == opp_id):
                    prior_h2h.append(pg)
                elif opp_name and (pg.home_name == opp_name or pg.away_name == opp_name):
                    prior_h2h.append(pg)

            if prior_h2h:
                desc.extend(ascii_rule(f"HEAD-TO-HEAD vs {opp_name}"))
                for pg in prior_h2h[-opp_limit:]:
                    desc.append(format_head_to_head_line(pg, team_id, opp_name, tz_name))
                desc.append("")

        # opponent games-to-date (before this matchup start)
        if opp_id is not None and opp_id in games_by_team and g.start_utc:
            opp_prior = [pg for pg in games_by_team[opp_id] if pg.start_utc and is_before_event(pg, g.start_utc)]
            if opp_prior:
                desc.extend(ascii_rule(f"{opp_name.upper()} GAMES-TO-DATE"))
                for pg in opp_prior[-opp_limit:]:
                    desc.append(format_game_line_for_opponent_view(pg, opp_id, tz_name))
                desc.append("")

        # check-in link
        desc.extend(ascii_rule("GAME CHECK-IN / REGISTRATION"))
        desc.append(f"Check in here: {checkin_url}")

        return ics_event(
            uid=stable_uid("btsh", f"{season_year}|team:{team_id}|game:{g.game_id}"),
            summary=summary,
            dtstart_local=dtstart_local,
            dtend_local=dtend_local,
            tz_name=tz_name,
            description_lines=desc,
            location=g.location,
            url=checkin_url,
        )

    # league-wide days -> all-day events
    def build_league_day_lines(d: dict) -> List[str]:
        title = str(pick(d, ["title", "name", "label", "type"], "League Day") or "League Day").strip()
        note = str(pick(d, ["note", "description", "details"], "") or "").strip()

        # try a date field
        date_raw = pick(d, ["date", "day", "game_day", "start"])
        dt = ensure_dt(date_raw)
        if dt:
            day_local = to_local(dt, tz_name)
        else:
            # if it's a date-only string
            if isinstance(date_raw, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", date_raw.strip()):
                day_local = datetime.fromisoformat(date_raw.strip())
            else:
                # if we cannot place it, skip
                return []

        desc: List[str] = []
        desc.extend(ascii_rule("LEAGUE-WIDE DAY"))
        desc.append(f"Season: {season_year}")
        if note:
            desc.append(note)

        return ics_allday_event(
            uid=stable_uid("btsh-day", f"{season_year}|{title}|{day_local.date().isoformat()}"),
            summary=f"[BTSH] {title}",
            day_local=day_local,
            description_lines=desc,
        )

    # Write each team file
    for tid, reg in sorted(team_regs.items(), key=lambda kv: kv[1].team_name.lower()):
        cal_lines: List[str] = []
        cal_lines.extend(calendar_header(f"BTSH {reg.team_name} ({season_year})"))

        # games relevant to this team
        for g in all_games:
            # include league-wide days separately
            if g.is_tbd() and include_tbd_on_all:
                # include TBD games on all calendars
                pass
            elif not g.involves_team_id(tid):
                continue

            # if it’s a TBD game but we’re not including them on all calendars, still include if it involves this team
            cal_lines.extend(build_team_event_lines(tid, g))

        if include_league_wide_days:
            for d in league_days:
                cal_lines.extend(build_league_day_lines(d))

        cal_lines.extend(calendar_footer())

        fname = f"btsh-{slugify(reg.team_name)}-season-{season_year}.ics"
        out_path = os.path.join(out_dir, fname)
        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(cal_lines) + "\n")

        print(f"Wrote {out_path}")

    # Write all-games calendar
    all_lines: List[str] = []
    all_lines.extend(calendar_header(f"BTSH All Games ({season_year})"))

    # put every game we parsed (including TBD/cancelled)
    for g in all_games:
        # simple summary
        summary = f"{g.home_name} vs {g.away_name}"
        if g.cancelled:
            summary = f"CANCELLED: {summary}"

        dtstart_local = to_local(g.start_utc, tz_name) if g.start_utc else None
        dtend_local = to_local(g.end_utc, tz_name) if g.end_utc else (dtstart_local + timedelta(hours=1) if dtstart_local else None)

        desc: List[str] = []
        desc.extend(ascii_rule("GAME INFO"))
        desc.append(f"Season: {season_year}")
        desc.append(f"Status: {g.status}{' (cancelled)' if g.cancelled else ''}")
        if dtstart_local:
            desc.append(f"Start ({tz_name}): {dtstart_local.strftime('%Y-%m-%d %H:%M %Z')}")
        if g.location:
            desc.append(f"Location: {g.location}")
        if g.note:
            desc.append(f"Note: {g.note}")
        desc.append("")
        desc.extend(ascii_rule("GAME CHECK-IN / REGISTRATION"))
        desc.append(f"Check in here: {checkin_url}")

        all_lines.extend(
            ics_event(
                uid=stable_uid("btsh", f"{season_year}|all|game:{g.game_id}"),
                summary=summary,
                dtstart_local=dtstart_local,
                dtend_local=dtend_local,
                tz_name=tz_name,
                description_lines=desc,
                location=g.location,
                url=checkin_url,
            )
        )

    if include_league_wide_days:
        for d in league_days:
            all_lines.extend(build_league_day_lines(d))

    all_lines.extend(calendar_footer())

    all_fname = f"btsh-all-games-season-{season_year}.ics"
    all_out_path = os.path.join(out_dir, all_fname)
    with open(all_out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(all_lines) + "\n")

    print(f"Wrote {all_out_path}")

if __name__ == "__main__":
    main()