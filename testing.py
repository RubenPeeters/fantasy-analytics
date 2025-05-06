from src.fantasy_analytics.get_teams import get_fb_ref_teams
from src.fantasy_analytics.custom_fbref import CustomFBRef

# ========== STATS ==========
stats_categories = {
    "standard": {"url": "stats", "html": "standard"},
    "goalkeeping": {"url": "keepers", "html": "keeper"},
    "advanced goalkeeping": {"url": "keepersadv", "html": "keeper_adv"},
    "shooting": {"url": "shooting", "html": "shooting"},
    "passing": {"url": "passing", "html": "passing"},
    "pass types": {"url": "passing_types", "html": "passing_types"},
    "goal and shot creation": {"url": "gca", "html": "gca"},
    "defensive": {"url": "defense", "html": "defense"},
    "possession": {"url": "possession", "html": "possession"},
    "playing time": {"url": "playingtime", "html": "playing_time"},
    "misc": {"url": "misc", "html": "misc"},
}
# each category will return three dataframes, namely:
# 1. squad stats
# 2. opponent stats
# 3. player stats

# ========== STATS ==========


if __name__ == "__main__":
    DB_FILE = "soccer_data.db"

    YEAR = "2023-2024"
    LEAGUE = "EPL"
    STAT: str | None = "standard"

    fb = CustomFBRef()
    stats = get_fb_ref_teams(fb, YEAR, LEAGUE, STAT)
    print(stats[2])
