import ScraperFC as sfc
from ScraperFC import FBref


def get_fb_ref_teams(fb: FBref, year: str, league: str, stats: str | None):
    if stats is None:
        data = fb.scrape_all_stats(year=year, league=league)
    else:
        data = fb.scrape_stats(year=year, league=league, stat_category=stats)
    # print(match.loc[0, "Away Player Stats"]["Summary"].head())
    return data


def get_fb_ref_matches(fb: FBref, year: str, league: str):
    data = fb.scrape_matches(year=year, league=league)
    # print(match.loc[0, "Away Player Stats"]["Summary"].head())
    return data


def get_valid_seasons_fb(fb: FBref, league: str):
    return fb.get_valid_seasons(league=league)


if __name__ == "__main__":
    YEAR = "2023-2024"
    LEAGUE = "Belgian Pro League"
    STAT: str | None = None

    fb = sfc.FBref()
    match_links = fb.get_match_links(YEAR, LEAGUE)

    print(match_links[0])
    match = fb.scrape_match(match_links[0])
    print(match.head())
