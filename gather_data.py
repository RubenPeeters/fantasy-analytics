import ScraperFC as sfc
from ScraperFC.fbref import comps
from ScraperFC import FBref
import pickle
from pathlib import Path
import os


def get_fb_ref(fb: FBref, year: str, league: str, stats: str | None):
    if stats is None:
        data = fb.scrape_all_stats(year=year, league=league)
    else:
        data = fb.scrape_stats(year=year, league=league, stat_category=stats)
    # print(match.loc[0, "Away Player Stats"]["Summary"].head())
    return data


def get_valid_seasons_fb(fb: FBref, league: str):
    return fb.get_valid_seasons(league=league)


if __name__ == "__main__":
    # YEAR = "2023-2024"
    # LEAGUE = "Belgian Pro League"
    STAT: str | None = None

    fb = sfc.FBref()
    for league in comps:
        seasons = get_valid_seasons_fb(fb, league)
        for year in seasons:
            data = get_fb_ref(fb, year, league, STAT)
            if STAT is None:
                file_path = Path.cwd() / "data" / league / year / f"{league}_{year}.pkl"
            else:
                file_path = (
                    Path.cwd() / "data" / league / year / STAT / f"{league}_{year}.pkl"
                )
            try:
                # Get the directory path from the file path
                directory_path = os.path.dirname(file_path)

                # Create the directory if it doesn't exist
                # exist_ok=True prevents an error if the directory already exists
                if directory_path:  # Check if directory_path is not an empty string
                    os.makedirs(directory_path, exist_ok=True)
                    print(f"Ensured directory '{directory_path}' exists.")

                with open(file_path, "wb") as f:  # Use 'wb' for write binary mode
                    pickle.dump(data, f)
                print(f"Objects successfully pickled and saved to {file_path}")
            except Exception as e:
                print(f"Error saving objects: {e}")
