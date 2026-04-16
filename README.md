# BTSH Team Calendars (ICS)

This repo builds `.ics` calendar files for BTSH:
- One calendar per registered team
- One master calendar with all games

## What it includes
- All games for the season (completed / scheduled / cancelled)
- Cancelled games are included and prefixed with `CANCELLED:` (configurable)
- Optional inclusion of placeholder games (`-` / `TBD` teams)
- Optional inclusion of non-game day types (for example holiday/other) as all-day events
- Each game event description includes:
  - Opponent games-to-date before the event start
  - Prior head-to-head matchups before the event start

## Config
Edit `config.yml` and set at least:

- `season_year`: season year to generate (for example `2026`)
- `output_dir`: output folder for generated `.ics` files

Common options:
- `include_placeholders`: include games with placeholder teams (`-` / `TBD`)
- `include_cancelled_games`: include cancelled games
- `team_calendar_day_types`: day types allowed in team calendars
- `master_calendar_day_types`: day types allowed in the master calendar
- `include_non_game_days_as_all_day_events`: include non-game days as all-day events
- `opponent_games_limit`: limit how many opponent prior games to show (`null` = no limit)
- `include_division_in_summary`: append division labels in event summaries
- `division_format`: `short` or `name`
- `cancelled_prefix`: summary prefix for cancelled events
- `master_file_name_template`: master calendar file naming template
- `team_file_prefix`: team calendar file prefix

## Run locally
```bash
pip install -r requirements.txt
python src/generate_ics.py
```

## Output
Generated files are written to `output_dir` (default: `docs/`), including:
- Team calendars: `<team_file_prefix>-<team-name>-season-<season_year>.ics`
- Master calendar: from `master_file_name_template` (default: `btsh-all-games-season-{year}.ics`)
