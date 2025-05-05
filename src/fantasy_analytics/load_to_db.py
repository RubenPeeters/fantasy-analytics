# --- Simple Function to Save a DataFrame to SQLite ---
import sqlite3
from pandas import DataFrame
from fantasy_analytics.data_cleaning import flatten_multiindex_columns


def save_dataframe_to_sqlite(
    df: DataFrame,
    table_name: str,
    db_file: str,
    year: str,
    league: str,
    if_exists: str = "replace",
) -> None:
    """
    Saves a pandas DataFrame to a specified SQLite table using df.to_sql().

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the table in the database.
        db_file (str): The path to the SQLite database file.
        year (str): The year to add as a column.
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
