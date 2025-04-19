import sqlite3
import pandas as pd
import pickle  # Import pickle
from pathlib import Path  # Import Path
from collections import Counter  # Import Counter for column name uniqueness
from ScraperFC.fbref import comps
import ScraperFC as sfc
from ScraperFC import FBref

# --- Configuration ---
DB_FILE = "soccer_data.db"


def get_valid_seasons_fb(fb: FBref, league: str):
    return fb.get_valid_seasons(league=league)


# --- Helper to flatten MultiIndex columns (copied from previous example) ---
def flatten_multiindex_columns(df):
    """
    Flattens a pandas DataFrame's MultiIndex columns into a single level.
    Handles 'Unnamed' columns by using the second level name.
    Cleans up special characters for use as database column names.
    Ensures resulting column names are unique.
    """
    if not isinstance(df.columns, pd.MultiIndex):
        # If columns are already flat, just clean them
        new_columns = [str(col) for col in df.columns]
    else:
        new_columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                # Handle 'Unnamed' columns, using the second level if available
                if str(col[0]).startswith("Unnamed:"):
                    new_col_name = (
                        str(col[1])
                        if str(col[1])
                        else "_".join(str(level) for level in col if str(level)).strip(
                            "_"
                        )
                    )
                else:
                    new_col_name = "_".join(
                        str(level) for level in col if str(level)
                    ).strip("_")
            else:
                new_col_name = str(col)

            new_col_name = (
                new_col_name.replace(" ", "_")
                .replace("-", "_")
                .replace("%", "Pct")
                .replace("+", "_plus_")
                .replace("#", "Num_")
                .replace("/", "_per_")
            )
            new_col_name = new_col_name.strip("_")
            if not new_col_name:
                new_col_name = f"col_{len(new_columns)}"

            new_columns.append(new_col_name)

    counts = Counter(new_columns)
    seen = {}
    unique_columns = []
    for col in new_columns:
        if counts[col] > 1:
            seen[col] = seen.get(col, 0) + 1
            unique_columns.append(f"{col}_{seen[col]}")
        else:
            unique_columns.append(col)

    return unique_columns  # Return the list of cleaned, unique column names


# --- Simple Function to Save a DataFrame to SQLite ---
def save_dataframe_to_sqlite(
    df, table_name, db_file, year, league, if_exists="replace"
):
    """
    Saves a pandas DataFrame to a specified SQLite table using df.to_sql().

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the table in the database.
        db_file (str): The path to the SQLite database file.
        year (int or str): The year to add as a column.
        league (str): The league name to add as a column.
        if_exists (str): How to behave if the table already exists.
                         'fail': Raise a ValueError.
                         'replace': Drop the table before inserting new values.
                         'append': Insert new values to the existing table.
                         Defaults to 'replace'.
    """
    if df.empty:
        print(f"DataFrame for table '{table_name}' is empty. Skipping save.")
        return

    conn = None  # Initialize connection to None
    try:
        # Connect to the SQLite database (creates the file if it doesn't exist)
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")

        # --- Prepare the DataFrame ---
        # 1. Flatten MultiIndex columns if they exist
        df_processed = df.copy()  # Work on a copy
        df_processed.columns = flatten_multiindex_columns(
            df_processed
        )  # Apply flattening and cleaning

        # 2. Add 'year' and 'league' columns
        df_processed["year"] = year
        df_processed["league"] = league

        # 3. Ensure column order for insertion (optional but good practice)
        # Place identifying columns first
        identifying_cols = []
        if "year" in df_processed.columns:
            identifying_cols.append("year")
        if "league" in df_processed.columns:
            identifying_cols.append("league")
        if "Player_ID" in df_processed.columns:
            identifying_cols.append("Player_ID")
        elif "Squad" in df_processed.columns:
            identifying_cols.append("Squad")
        # Add other columns that are not identifying cols
        other_cols = [
            col for col in df_processed.columns if col not in identifying_cols
        ]
        final_column_order = identifying_cols + other_cols
        df_processed = df_processed[final_column_order]

        # --- Use df.to_sql() to save the DataFrame ---
        # pandas handles table creation (based on DataFrame dtypes) and insertion
        # It maps pandas dtypes to SQLite types automatically (e.g., int64 -> INTEGER, float64 -> REAL, object -> TEXT)
        df_processed.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,  # 'replace' will drop the table if it exists
            index=False,  # Don't write the DataFrame index as a column
        )

        print(
            f"Successfully saved {len(df_processed)} rows to table '{table_name}'. (if_exists='{if_exists}')"
        )

    except sqlite3.Error as e:
        print(f"Database error while saving '{table_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while saving '{table_name}': {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    fb = sfc.FBref()
    STAT = None

    for league in comps:
        seasons = get_valid_seasons_fb(fb, league)
        for year in seasons:
            if STAT is None:
                file_path = Path.cwd() / "data" / league / year / f"{league}_{year}.pkl"
            else:
                file_path = (
                    Path.cwd() / "data" / league / year / STAT / f"{league}_{year}.pkl"
                )

            loaded_data = None

            try:
                with open(file_path, "rb") as f:  # Use 'rb' for read binary mode
                    loaded_data = pickle.load(f)
                print(f"\nObjects successfully unpickled from {file_path}")
                # print("Loaded Data:")
                # print(loaded_data)  # Uncomment to print the full loaded data
            except FileNotFoundError:
                print(f"\nError loading objects: File not found at {file_path}")
                continue
            except Exception as e:
                print(f"\nError loading objects: {e}")

            if STAT is None:
                print(loaded_data.keys())

                print(loaded_data["standard"][0].head(5))
                print(loaded_data["standard"][0].columns)

            if loaded_data is not None:
                for stat_type, dataframes_tuple in loaded_data.items():
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
                                table_suffix = (
                                    f"item_{i}"  # Fallback for unexpected DFs
                                )

                            table_name = f"{base_table_name}_{table_suffix}_stats"

                            # Call the save function for each DataFrame
                            # Use if_exists='append' if you are adding data over multiple runs/files
                            # Use if_exists='replace' if you want to overwrite the table each time
                            save_dataframe_to_sqlite(
                                df=df,
                                table_name=table_name,
                                db_file=DB_FILE,
                                year=year,  # Use your loaded year
                                league=league,  # Use your loaded league
                                if_exists="append",  # Or 'replace' as needed
                            )
                        else:
                            print(
                                f"Warning: Item {i} for '{stat_type}' is not a DataFrame. Skipping."
                            )
