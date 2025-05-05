import os
from pathlib import Path
import pickle
from fantasy_analytics.get_teams import get_fb_ref_teams
from fantasy_analytics.load_to_db import save_dataframe_to_sqlite
from src.fantasy_analytics.custom_fbref import CustomFBRef
import pandas as pd


if __name__ == "__main__":
    DB_FILE = "soccer_data.db"

    YEAR = "2023-2024"
    LEAGUE = "EPL"
    STAT: str | None = None

    fb = CustomFBRef()
    match_links = fb.get_match_links(YEAR, LEAGUE)

    # match = fb.scrape_match(match_links[0])
    # match = fb.scrape_match_lineups(match_links[0])

    # seasons = get_valid_seasons_fb(fb, LEAGUE)
    # for year in seasons:
    team_data = get_fb_ref_teams(fb, YEAR, LEAGUE, STAT)
    if STAT is None:
        file_path = (
            Path.cwd() / "data" / LEAGUE / YEAR / "team" / f"{LEAGUE}_{YEAR}.pkl"
        )
    else:
        file_path = (
            Path.cwd() / "data" / LEAGUE / YEAR / STAT / "team" / f"{LEAGUE}_{YEAR}.pkl"
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
            pickle.dump(team_data, f)
        print(f"Objects successfully pickled and saved to {file_path}")
    except Exception as e:
        print(f"Error saving objects: {e}")
    if team_data is not None:
        for stat_type, dataframes_tuple in team_data.items():
            base_table_name = stat_type.replace(" ", "_")
            for i, df in enumerate(dataframes_tuple):
                if isinstance(df, pd.DataFrame):
                    # Determine table name based on index and stat type (e.g., squad, player, opponent)
                    table_suffix = ""
                    if i == 0:
                        table_suffix = "squad"
                    elif i == 1:
                        table_suffix = "opponent"
                    elif i == 2:
                        table_suffix = "player"
                    else:
                        table_suffix = f"item_{i}"  # Fallback for unexpected DFs

                    table_name = f"{base_table_name}_{table_suffix}_stats"

                    # Call the save function for each DataFrame
                    # Use if_exists='append' if you are adding data over multiple runs/files
                    # Use if_exists='replace' if you want to overwrite the table each time
                    save_dataframe_to_sqlite(
                        df=df,
                        table_name=table_name,
                        db_file=DB_FILE,
                        year=YEAR,  # Use your loaded year
                        league=LEAGUE,  # Use your loaded league
                        if_exists="append",  # Or 'replace' as needed
                    )
                else:
                    print(
                        f"Warning: Item {i} for '{stat_type}' is not a DataFrame. Skipping."
                    )
