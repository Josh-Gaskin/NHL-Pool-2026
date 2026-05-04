"""
update_stats.py
---------------
Fetches 2025-26 playoff stats for all 60 pool players from the NHL API
and writes results to Firebase Firestore.

Uses the /v1/player/{id}/landing endpoint which always contains current
careerTotals including live playoff stats — much more reliable than
the /game-log/ endpoint which can lag or return empty data early in playoffs.

Run on a schedule via GitHub Actions every 30 minutes.
"""

import os
import json
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

# ── Firebase init ─────────────────────────────────────────────────────────────
service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ── All 60 pool players ───────────────────────────────────────────────────────
PLAYERS = [
    # Group 1
    {"name": "Nikita Kucherov",       "team": "TBL", "type": "skater", "id": 8476453, "group": 1},
    {"name": "Connor McDavid",         "team": "EDM", "type": "skater", "id": 8478402, "group": 1},
    {"name": "Nathan MacKinnon",       "team": "COL", "type": "skater", "id": 8477492, "group": 1},
    {"name": "Leon Draisaitl",         "team": "EDM", "type": "skater", "id": 8477934, "group": 1},
    # Group 2
    {"name": "David Pastrnak",         "team": "BOS", "type": "skater", "id": 8477956, "group": 2},
    {"name": "Martin Necas",           "team": "COL", "type": "skater", "id": 8480762, "group": 2},
    {"name": "Mikko Rantanen",         "team": "DAL", "type": "skater", "id": 8478420, "group": 2},
    {"name": "Jack Eichel",            "team": "VGK", "type": "skater", "id": 8478403, "group": 2},
    # Group 3
    {"name": "Nick Suzuki",            "team": "MTL", "type": "skater", "id": 8480800, "group": 3},
    {"name": "Mark Stone",             "team": "VGK", "type": "skater", "id": 8475913, "group": 3},
    {"name": "Jason Robertson",        "team": "DAL", "type": "skater", "id": 8481528, "group": 3},
    {"name": "Kirill Kaprizov",        "team": "MIN", "type": "skater", "id": 8481600, "group": 3},
    # Group 4
    {"name": "Matt Boldy",             "team": "MIN", "type": "skater", "id": 8482109, "group": 4},
    {"name": "Sidney Crosby",          "team": "PIT", "type": "skater", "id": 8471675, "group": 4},
    {"name": "Cole Caufield",          "team": "MTL", "type": "skater", "id": 8481540, "group": 4},
    {"name": "Jake Guentzel",          "team": "TBL", "type": "skater", "id": 8477404, "group": 4},
    # Group 5
    {"name": "Brandon Hagel",          "team": "TBL", "type": "skater", "id": 8480801, "group": 5},
    {"name": "Evgeni Malkin",          "team": "PIT", "type": "skater", "id": 8471215, "group": 5},
    {"name": "Clayton Keller",         "team": "UTA", "type": "skater", "id": 8479343, "group": 5},
    {"name": "Wyatt Johnston",         "team": "DAL", "type": "skater", "id": 8483515, "group": 5},
    # Group 6
    {"name": "Tage Thompson",          "team": "BUF", "type": "skater", "id": 8480993, "group": 6},
    {"name": "Sebastian Aho",          "team": "CAR", "type": "skater", "id": 8478427, "group": 6},
    {"name": "Brady Tkachuk",          "team": "OTT", "type": "skater", "id": 8481528, "group": 6},
    {"name": "Mitch Marner",           "team": "VGK", "type": "skater", "id": 8478483, "group": 6},
    # Group 7
    {"name": "Tim Stützle",            "team": "OTT", "type": "skater", "id": 8482116, "group": 7},
    {"name": "Artemi Panarin",         "team": "LAK", "type": "skater", "id": 8478550, "group": 7},
    {"name": "Leo Carlsson",           "team": "ANA", "type": "skater", "id": 8484144, "group": 7},
    {"name": "Travis Konecny",         "team": "PHI", "type": "skater", "id": 8479365, "group": 7},
    # Group 8
    {"name": "Dylan Guenther",         "team": "UTA", "type": "skater", "id": 8483453, "group": 8},
    {"name": "Andrei Svechnikov",      "team": "CAR", "type": "skater", "id": 8480830, "group": 8},
    {"name": "Zach Hyman",             "team": "EDM", "type": "skater", "id": 8475786, "group": 8},
    {"name": "Cutter Gauthier",        "team": "ANA", "type": "skater", "id": 8484882, "group": 8},
    # Group 9
    {"name": "Juraj Slafkovský",       "team": "MTL", "type": "skater", "id": 8484145, "group": 9},
    {"name": "Adrian Kempe",           "team": "LAK", "type": "skater", "id": 8477960, "group": 9},
    {"name": "Seth Jarvis",            "team": "CAR", "type": "skater", "id": 8482655, "group": 9},
    {"name": "Alex Tuch",              "team": "BUF", "type": "skater", "id": 8478450, "group": 9},
    # Group 10
    {"name": "Porter Martone",         "team": "PHI", "type": "skater", "id": 8487950, "group": 10},
    {"name": "Nick Schmaltz",          "team": "UTA", "type": "skater", "id": 8478057, "group": 10},
    {"name": "Drake Batherson",        "team": "OTT", "type": "skater", "id": 8481533, "group": 10},
    {"name": "Morgan Geekie",          "team": "BOS", "type": "skater", "id": 8478476, "group": 10},
    # Group 11
    {"name": "Cale Makar",             "team": "COL", "type": "skater", "id": 8480069, "group": 11},
    {"name": "Evan Bouchard",          "team": "EDM", "type": "skater", "id": 8480144, "group": 11},
    {"name": "Quinn Hughes",           "team": "VAN", "type": "skater", "id": 8480800, "group": 11},
    {"name": "Rasmus Dahlin",          "team": "BUF", "type": "skater", "id": 8480769, "group": 11},
    # Group 12
    {"name": "Lane Hutson",            "team": "MTL", "type": "skater", "id": 8487947, "group": 12},
    {"name": "Darren Raddysh",         "team": "TBL", "type": "skater", "id": 8478438, "group": 12},
    {"name": "Shayne Gostisbehere",    "team": "CAR", "type": "skater", "id": 8477494, "group": 12},
    {"name": "Erik Karlsson",          "team": "PIT", "type": "skater", "id": 8474578, "group": 12},
    # Group 13
    {"name": "Charlie McAvoy",         "team": "BOS", "type": "skater", "id": 8479325, "group": 13},
    {"name": "John Carlson",           "team": "ANA", "type": "skater", "id": 8474270, "group": 13},
    {"name": "Jake Sanderson",         "team": "OTT", "type": "skater", "id": 8483411, "group": 13},
    {"name": "Miro Heiskanen",         "team": "DAL", "type": "skater", "id": 8481522, "group": 13},
    # Group 14 — Goalies
    {"name": "Scott Wedgewood",        "team": "COL", "type": "goalie", "id": 8476419, "group": 14},
    {"name": "Jake Oettinger",         "team": "DAL", "type": "goalie", "id": 8480382, "group": 14},
    {"name": "Andrei Vasilevskiy",     "team": "TBL", "type": "goalie", "id": 8476883, "group": 14},
    {"name": "Ukko-Pekka Luukkonen",   "team": "BUF", "type": "goalie", "id": 8481533, "group": 14},
    # Group 15 — Goalies
    {"name": "Karel Vejmelka",         "team": "UTA", "type": "goalie", "id": 8480353, "group": 15},
    {"name": "Carter Hart",            "team": "VGK", "type": "goalie", "id": 8479361, "group": 15},
    {"name": "Linus Ullmark",          "team": "OTT", "type": "goalie", "id": 8476945, "group": 15},
    {"name": "Stuart Skinner",         "team": "PIT", "type": "goalie", "id": 8480407, "group": 15},
]

SEASON_ID = "20252026"
HEADERS   = {"User-Agent": "NHL-Pool-Tracker/1.0", "Accept": "application/json"}


def fetch_player_stats(player: dict) -> dict | None:
    """
    Use the /v1/player/{id}/landing endpoint.
    This always contains careerTotals with a 'playoffs' section
    broken down by season — we find the 20252026 entry.
    """
    url = f"https://api-web.nhle.com/v1/player/{player['id']}/landing"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        # careerTotals has a 'playoffs' list of seasons
        playoffs = data.get("careerTotals", {}).get("playoffs", {})

        if player["type"] == "skater":
            # The playoffs section is a single dict (aggregated career)
            # We need the seasonTotals list instead to find the specific season
            season_totals = data.get("seasonTotals", [])
            goals = assists = pim = 0
            for entry in season_totals:
                if (str(entry.get("season", "")) == SEASON_ID
                        and entry.get("gameTypeId") == 3):
                    goals   = entry.get("goals", 0)
                    assists = entry.get("assists", 0)
                    pim     = entry.get("pim", 0)
                    break
            return {"goals": goals, "assists": assists, "pim": float(pim),
                    "wins": 0, "shutouts": 0}

        else:  # goalie
            season_totals = data.get("seasonTotals", [])
            wins = shutouts = 0
            for entry in season_totals:
                if (str(entry.get("season", "")) == SEASON_ID
                        and entry.get("gameTypeId") == 3):
                    wins     = entry.get("wins", 0)
                    shutouts = entry.get("shutouts", 0)
                    break
            return {"goals": 0, "assists": 0, "pim": 0.0,
                    "wins": wins, "shutouts": shutouts}

    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def fetch_eliminated_teams() -> list:
    """Read the playoff bracket and return eliminated team abbreviations.

    The API returns a flat 'series' list at the top level (no 'rounds' wrapper).
    Each series has:
      - losingTeamId: the numeric ID of the eliminated team
      - topSeedTeam.id / topSeedTeam.abbrev
      - bottomSeedTeam.id / bottomSeedTeam.abbrev
      - topSeedWins / bottomSeedWins
    A series is complete when either topSeedWins >= 4 or bottomSeedWins >= 4.
    """
    url = "https://api-web.nhle.com/v1/playoff-bracket/2026"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        eliminated = set()

        # Flat series list — no rounds wrapper
        for series in data.get("series", []):
            top_w   = series.get("topSeedWins", 0)
            bot_w   = series.get("bottomSeedWins", 0)
            losing_id = series.get("losingTeamId")
            top_team  = series.get("topSeedTeam", {})
            bot_team  = series.get("bottomSeedTeam", {})

            # Method 1: losingTeamId is set when series is complete
            if losing_id:
                if top_team.get("id") == losing_id:
                    eliminated.add(top_team["abbrev"])
                elif bot_team.get("id") == losing_id:
                    eliminated.add(bot_team["abbrev"])

            # Method 2: fallback — check win counts directly
            elif top_w >= 4 and bot_team.get("abbrev"):
                eliminated.add(bot_team["abbrev"])
            elif bot_w >= 4 and top_team.get("abbrev"):
                eliminated.add(top_team["abbrev"])

        print(f"  Eliminated teams: {sorted(eliminated) if eliminated else 'none yet'}")
        return list(eliminated)
    except Exception as e:
        print(f"  Bracket ERROR: {e}")
        return []


def main():
    now = datetime.now(timezone.utc)
    print(f"=== NHL Pool Stats Update — {now.strftime('%Y-%m-%d %H:%M UTC')} ===\n")

    # ── Fetch all player stats ────────────────────────────────────────────────
    stats_batch = {}
    for p in PLAYERS:
        print(f"Fetching {p['name']} ({p['team']}, {p['type']}, id={p['id']})...")
        result = fetch_player_stats(p)

        if result is not None:
            stats_batch[p["name"]] = {
                **result,
                "team":  p["team"],
                "type":  p["type"],
                "group": p["group"],
                "nhlId": p["id"],
            }
            if p["type"] == "skater":
                pts = result["goals"]*3 + result["assists"]*2 + result["pim"]*0.5
                print(f"  G:{result['goals']} A:{result['assists']} "
                      f"PIM:{result['pim']} → {pts:.1f} pool pts")
            else:
                pts = result["wins"]*3 + result["shutouts"]*3
                print(f"  W:{result['wins']} SO:{result['shutouts']} "
                      f"→ {pts:.1f} pool pts")
        else:
            print(f"  SKIPPED (API error)")

    # ── Fetch eliminated teams ────────────────────────────────────────────────
    print("\nFetching playoff bracket...")
    eliminated = fetch_eliminated_teams()

    # ── Write everything to Firestore as a single document ───────────────────
    print(f"\nWriting {len(stats_batch)} players to Firestore...")
    db.collection("pool").document("stats").set({
        "players":        stats_batch,
        "eliminatedTeams": eliminated,
        "lastUpdated":    now.isoformat(),
    })
    print(f"\n✅ Done — {len(stats_batch)}/60 players written.")


if __name__ == "__main__":
    main()
