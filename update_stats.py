"""
update_stats.py
---------------
Fetches playoff stats for all 60 pool players directly from the NHL API
(no CORS restrictions server-side) and writes results to Firebase Firestore.

Run on a schedule via GitHub Actions every 30 minutes.
"""

import os
import json
import requests
import firebase_admin
from firebase_admin import credentials, firestore

# ─── Firebase init ────────────────────────────────────────────────────────────
# GitHub Actions passes the service account JSON as an environment variable.
service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ─── All 60 pool players with their NHL player IDs ────────────────────────────
PLAYERS = [
    # Group 1
    {"name": "Nikita Kucherov", "team": "TBL", "type": "skater", "id": 8476453, "group": 1},
    {"name": "Connor McDavid", "team": "EDM", "type": "skater", "id": 8478402, "group": 1},
    {"name": "Nathan MacKinnon", "team": "COL", "type": "skater", "id": 8477492, "group": 1},
    {"name": "Leon Draisaitl", "team": "EDM", "type": "skater", "id": 8477934, "group": 1},
    # Group 2
    {"name": "David Pastrnak", "team": "BOS", "type": "skater", "id": 8477956, "group": 2},
    {"name": "Martin Necas", "team": "COL", "type": "skater", "id": 8480039, "group": 2},
    {"name": "Mikko Rantanen", "team": "DAL", "type": "skater", "id": 8478420, "group": 2},
    {"name": "Jack Eichel", "team": "VGK", "type": "skater", "id": 8478403, "group": 2},
    # Group 3
    {"name": "Nick Suzuki", "team": "MTL", "type": "skater", "id": 8480018, "group": 3},
    {"name": "Mark Stone", "team": "VGK", "type": "skater", "id": 8475913, "group": 3},
    {"name": "Jason Robertson", "team": "DAL", "type": "skater", "id": 8480027, "group": 3},
    {"name": "Kirill Kaprizov", "team": "MIN", "type": "skater", "id": 8478864, "group": 3},
    # Group 4
    {"name": "Matt Boldy", "team": "MIN", "type": "skater", "id": 8481557, "group": 4},
    {"name": "Sidney Crosby", "team": "PIT", "type": "skater", "id": 8471675, "group": 4},
    {"name": "Cole Caufield", "team": "MTL", "type": "skater", "id": 8481540, "group": 4},
    {"name": "Jake Guentzel", "team": "TBL", "type": "skater", "id": 8477404, "group": 4},
    # Group 5
    {"name": "Brandon Hagel", "team": "TBL", "type": "skater", "id": 8479542, "group": 5},
    {"name": "Evgeni Malkin", "team": "PIT", "type": "skater", "id": 8471215, "group": 5},
    {"name": "Clayton Keller", "team": "UTA", "type": "skater", "id": 8479343, "group": 5},
    {"name": "Wyatt Johnston", "team": "DAL", "type": "skater", "id": 8482740, "group": 5},
    # Group 6
    {"name": "Tage Thompson", "team": "BUF", "type": "skater", "id": 8479420, "group": 6},
    {"name": "Sebastian Aho", "team": "CAR", "type": "skater", "id": 8478427, "group": 6},
    {"name": "Brady Tkachuk", "team": "OTT", "type": "skater", "id": 8480801, "group": 6},
    {"name": "Mitch Marner", "team": "VGK", "type": "skater", "id": 8478483, "group": 6},
    # Group 7
    {"name": "Tim Stützle", "team": "OTT", "type": "skater", "id": 8482116, "group": 7},
    {"name": "Artemi Panarin", "team": "LAK", "type": "skater", "id": 8478550, "group": 7},
    {"name": "Leo Carlsson", "team": "ANA", "type": "skater", "id": 8484153, "group": 7},
    {"name": "Travis Konecny", "team": "PHI", "type": "skater", "id": 8478439, "group": 7},
    # Group 8
    {"name": "Dylan Guenther", "team": "UTA", "type": "skater", "id": 8482699, "group": 8},
    {"name": "Andrei Svechnikov", "team": "CAR", "type": "skater", "id": 8480830, "group": 8},
    {"name": "Zach Hyman", "team": "EDM", "type": "skater", "id": 8475786, "group": 8},
    {"name": "Cutter Gauthier", "team": "ANA", "type": "skater", "id": 8483445, "group": 8},
    # Group 9
    {"name": "Juraj Slafkovský", "team": "MTL", "type": "skater", "id": 8483515, "group": 9},
    {"name": "Adrian Kempe", "team": "LAK", "type": "skater", "id": 8477960, "group": 9},
    {"name": "Seth Jarvis", "team": "CAR", "type": "skater", "id": 8482093, "group": 9},
    {"name": "Alex Tuch", "team": "BUF", "type": "skater", "id": 8477949, "group": 9},
    # Group 10
    {"name": "Porter Martone", "team": "PHI", "type": "skater", "id": 8485406, "group": 10},
    {"name": "Nick Schmaltz", "team": "UTA", "type": "skater", "id": 8477951, "group": 10},
    {"name": "Drake Batherson", "team": "OTT", "type": "skater", "id": 8480208, "group": 10},
    {"name": "Morgan Geekie", "team": "BOS", "type": "skater", "id": 8479987, "group": 10},
    # Group 11
    {"name": "Cale Makar", "team": "COL", "type": "skater", "id": 8480069, "group": 11},
    {"name": "Evan Bouchard", "team": "EDM", "type": "skater", "id": 8480803, "group": 11},
    {"name": "Quinn Hughes", "team": "MIN", "type": "skater", "id": 8480800, "group": 11},
    {"name": "Rasmus Dahlin", "team": "BUF", "type": "skater", "id": 8480839, "group": 11},
    # Group 12
    {"name": "Lane Hutson", "team": "MTL", "type": "skater", "id": 8483457, "group": 12},
    {"name": "Darren Raddysh", "team": "TBL", "type": "skater", "id": 8478178, "group": 12},
    {"name": "Shayne Gostisbehere", "team": "CAR", "type": "skater", "id": 8476906, "group": 12},
    {"name": "Erik Karlsson", "team": "PIT", "type": "skater", "id": 8474578, "group": 12},
    # Group 13
    {"name": "Charlie McAvoy", "team": "BOS", "type": "skater", "id": 8479325, "group": 13},
    {"name": "John Carlson", "team": "ANA", "type": "skater", "id": 8474590, "group": 13},
    {"name": "Jake Sanderson", "team": "OTT", "type": "skater", "id": 8482105, "group": 13},
    {"name": "Miro Heiskanen", "team": "DAL", "type": "skater", "id": 8480036, "group": 13},
    # Group 14
    {"name": "Scott Wedgewood", "team": "COL", "type": "goalie", "id": 8475809, "group": 14},
    {"name": "Jake Oettinger", "team": "DAL", "type": "goalie", "id": 8479979, "group": 14},
    {"name": "Andrei Vasilevskiy", "team": "TBL", "type": "goalie", "id": 8476883, "group": 14},
    {"name": "Ukko-Pekka Luukkonen", "team": "BUF", "type": "goalie", "id": 8480045, "group": 14},
    # Group 15
    {"name": "Karel Vejmelka", "team": "UTA", "type": "goalie", "id": 8478872, "group": 15},
    {"name": "Carter Hart", "team": "VGK", "type": "goalie", "id": 8479394, "group": 15},
    {"name": "Linus Ullmark", "team": "OTT", "type": "goalie", "id": 8476999, "group": 15},
    {"name": "Stuart Skinner", "team": "PIT", "type": "goalie", "id": 8479973, "group": 15},
]

SEASON = "20252026"
HEADERS = {"User-Agent": "NHL-Pool-Tracker/1.0"}

def fetch_skater_stats(player_id: int) -> dict:
    """Fetch playoff game log for a skater and sum goals, assists, PIM."""
    url = f"https://api-web.nhle.com/v1/player/{player_id}/game-log/{SEASON}/3"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        goals, assists, pim = 0, 0, 0.0
        for game in data.get("gameLog", []):
            goals   += game.get("goals", 0)
            assists += game.get("assists", 0)
            pim     += game.get("pim", 0)
        return {"goals": goals, "assists": assists, "pim": pim, "wins": 0, "shutouts": 0}
    except Exception as e:
        print(f"  ERROR fetching skater {player_id}: {e}")
        return None

def fetch_goalie_stats(player_id: int) -> dict:
    """Fetch playoff game log for a goalie and sum wins and shutouts."""
    url = f"https://api-web.nhle.com/v1/player/{player_id}/game-log/{SEASON}/3"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        wins, shutouts = 0, 0
        for game in data.get("gameLog", []):
            decision = game.get("decision", "")
            if decision == "W":
                wins += 1
            toi = game.get("toi", "0:00")
            shots_against = game.get("shotsAgainst", 1)
            goals_against = game.get("goalsAgainst", 0)
            # A shutout: played meaningful time and allowed 0 goals
            if goals_against == 0 and shots_against > 0 and decision in ("W", "O", "L"):
                # Only count as shutout if they played the full game
                # (toi typically "60:00" or similar for full game)
                mins = int(toi.split(":")[0]) if ":" in toi else 0
                if mins >= 55:
                    shutouts += 1
        return {"goals": 0, "assists": 0, "pim": 0, "wins": wins, "shutouts": shutouts}
    except Exception as e:
        print(f"  ERROR fetching goalie {player_id}: {e}")
        return None

def fetch_eliminated_teams() -> list:
    """Read the playoff bracket and return list of eliminated team abbreviations."""
    url = "https://api-web.nhle.com/v1/playoff-bracket/2026"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        eliminated = set()
        rounds = data.get("rounds", data.get("bracket", {}).get("rounds", []))
        for rnd in rounds:
            for series in rnd.get("series", []):
                if series.get("loser", {}).get("abbrev"):
                    eliminated.add(series["loser"]["abbrev"])
                top_wins = series.get("topSeedWins", series.get("topSeedSeriesWins", 0))
                bot_wins = series.get("bottomSeedWins", series.get("bottomSeedSeriesWins", 0))
                if top_wins >= 4 and series.get("bottomSeed", {}).get("abbrev"):
                    eliminated.add(series["bottomSeed"]["abbrev"])
                if bot_wins >= 4 and series.get("topSeed", {}).get("abbrev"):
                    eliminated.add(series["topSeed"]["abbrev"])
        return list(eliminated)
    except Exception as e:
        print(f"  ERROR fetching bracket: {e}")
        return []

def main():
    from datetime import datetime, timezone

    print(f"=== NHL Pool Stats Update — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} ===\n")

    # ── Fetch all player stats ────────────────────────────────────────────────
    stats_batch = {}
    for p in PLAYERS:
        print(f"Fetching {p['name']} ({p['team']}, {p['type']})...")
        if p["type"] == "skater":
            result = fetch_skater_stats(p["id"])
        else:
            result = fetch_goalie_stats(p["id"])

        if result is not None:
            stats_batch[p["name"]] = {
                **result,
                "team": p["team"],
                "type": p["type"],
                "group": p["group"],
                "nhId": p["id"],
            }
            if p["type"] == "skater":
                pts = result["goals"]*3 + result["assists"]*2 + result["pim"]*0.5
                print(f"  G:{result['goals']} A:{result['assists']} PIM:{result['pim']} → {pts:.1f} pool pts")
            else:
                pts = result["wins"]*3 + result["shutouts"]*3
                print(f"  W:{result['wins']} SO:{result['shutouts']} → {pts:.1f} pool pts")
        else:
            print(f"  Skipped (error)")

    # ── Fetch eliminated teams ────────────────────────────────────────────────
    print("\nFetching playoff bracket for eliminations...")
    eliminated = fetch_eliminated_teams()
    print(f"  Eliminated teams: {eliminated if eliminated else 'none yet'}")

    # ── Write to Firestore ────────────────────────────────────────────────────
    print("\nWriting to Firestore...")

    # Write all player stats as a single document for efficiency
    db.collection("pool").document("stats").set({
        "players": stats_batch,
        "eliminatedTeams": eliminated,
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
    })

    print(f"\n✅ Done — {len(stats_batch)} players written to Firestore.")

if __name__ == "__main__":
    main()
