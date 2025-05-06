from .custom_fbref import CustomFBRef


def get_valid_seasons_fb(fb: CustomFBRef, league: str) -> list:
    """
    Get valid seasons for a given league using the CustomFBRef instance.
    Args:
        fb (CustomFBRef): Instance of the CustomFBRef class.
        league (str): League to get valid seasons for.
    Returns:
        list: List of valid seasons for the specified league.
    """
    return fb.get_valid_seasons(league=league)
