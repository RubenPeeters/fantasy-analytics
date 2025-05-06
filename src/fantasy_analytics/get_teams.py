from src.fantasy_analytics.custom_fbref import CustomFBRef


def get_fb_ref_teams(
    fb: CustomFBRef, year: str, league: str, stats: str | None
) -> dict:
    """
    Get FBRef team stats for a given year and league.
    Args:
        fb (CustomFBRef): Instance of the CustomFBRef class.
        year (str): Year to scrape data for.
        league (str): League to scrape data for.
        stats (str | None): Specific stat category to scrape. If None, scrape all stats.
    Returns:
        dict: Dictionary containing scraped team stats.
    """
    if stats is None:
        data = fb.scrape_all_stats(year=year, league=league)
    else:
        data = fb.scrape_stats(year=year, league=league, stat_category=stats)
    return data
