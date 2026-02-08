# BTSH Team Calendars (ICS)

This repo builds an `.ics` calendar file for **every BTSH team** for a given season.

## What it includes
- All games for the season (completed / scheduled / cancelled)
- Cancelled games are included and labeled `[CANCELLED]`
- League-wide placeholder days (holidays / makeups / other non-game days) are included on every team calendar
- Each game event description includes:
  - Opponent’s games with start times **before this event’s start**
    - Includes completed games with results when available
    - Includes scheduled games (no score yet)
    - Includes cancelled games labeled as such
  - Prior matchups between the two teams

## Config
Edit `config.yml`:

- `season`: season number (2 = 2025, 3 = 2026, etc.)
- `opponent_recent_limit`: number of opponent games to list

## Run locally
```bash
pip install -r requirements.txt
python src/generate_ics.py
