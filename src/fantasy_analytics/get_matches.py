from .custom_fbref import CustomFBRef


def get_fb_ref_matches(fb: CustomFBRef, year: str, league: str) -> dict:
    """
    Get FBRef match data for a given year and league.
    Args:
        fb (CustomFBRef): Instance of the CustomFBRef class.
        year (str): Year to scrape data for.
        league (str): League to scrape data for.
    Returns:
        dict: Dictionary containing scraped match data.
    """
    data = fb.scrape_matches(year=year, league=league)
    return data
