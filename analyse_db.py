import sqlite3
import pandas as pd
import numpy as np  # Import numpy for numerical operations and NaN handling

# --- Configuration ---
DB_FILE = "soccer_data.db"  # Make sure this matches your database file

# --- Database Connection and Data Loading ---

if __name__ == "__main__":
    conn = None  # Initialize connection to None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_FILE)
        print(f"Connected to database: {DB_FILE}")

        # --- Load data from relevant tables ---
        # We need to load data from multiple player-level tables and merge them.
        # Make sure the table names and column names match exactly what's in your DB.
        # Use the flattened column names from your table schemas.

        # Load Player Standard Stats
        try:
            query_standard = """
            SELECT
                "Player_ID", "year", "league", "Player", "Squad", "Pos",
                "Playing_Time_Min", "Playing_Time_90s",
                "Performance_Gls", "Performance_Ast", "Performance_PK", "Performance_PKatt",
                "Performance_CrdY", "Performance_CrdR"
            FROM "standard_player_stats" -- Corrected table name
            """
            df_standard = pd.read_sql_query(query_standard, conn)
            print(f"Loaded {len(df_standard)} rows from standard_player_stats.")
        except pd.io.sql.DatabaseError as e:
            print(f"Error loading standard_player_stats: {e}")
            df_standard = pd.DataFrame()  # Create empty DataFrame if loading fails

        # Load Player Goalkeeping Stats (needed for Penalty Saves, Goals Against GK, Saves)
        try:
            query_goalkeeping = """
            SELECT
                "Player_ID", "year", "league",
                "Penalty_Kicks_PKsv", -- Penalty Saves
                "Performance_GA", -- Goals Against (for GK)
                "Performance_Saves" -- Saves (for GK)
            FROM "goalkeeping_player_stats" -- Corrected table name
            """
            df_goalkeeping = pd.read_sql_query(query_goalkeeping, conn)
            print(f"Loaded {len(df_goalkeeping)} rows from goalkeeping_player_stats.")
        except pd.io.sql.DatabaseError as e:
            print(f"Error loading goalkeeping_player_stats: {e}")
            df_goalkeeping = pd.DataFrame()  # Create empty DataFrame if loading fails

        # Load Player Shooting Stats (needed for Shots Off Target)
        try:
            query_shooting = """
            SELECT
                "Player_ID", "year", "league",
                "Standard_Sh", -- Total Shots
                "Standard_SoT" -- Shots on Target
            FROM "shooting_player_stats" -- Corrected table name
            """
            df_shooting = pd.read_sql_query(query_shooting, conn)
            print(f"Loaded {len(df_shooting)} rows from shooting_player_stats.")
        except pd.io.sql.DatabaseError as e:
            print(f"Error loading shooting_player_stats: {e}")
            df_shooting = pd.DataFrame()  # Create empty DataFrame if loading fails

        # Load Player Misc Stats (needed for Own Goals)
        try:
            query_misc = """
            SELECT
                "Player_ID", "year", "league",
                "Performance_OG" -- Own Goals
            FROM "misc_player_stats" -- Corrected table name
            """
            df_misc = pd.read_sql_query(query_misc, conn)
            print(f"Loaded {len(df_misc)} rows from misc_player_stats.")
        except pd.io.sql.DatabaseError as e:
            print(f"Error loading misc_player_stats: {e}")
            df_misc = pd.DataFrame()  # Create empty DataFrame if loading fails

        # Load Player Defensive Stats (needed for Errors, Interceptions, maybe Headed Clearances/Duels)
        try:
            query_defensive = """
            SELECT
                "Player_ID", "year", "league",
                "Err", -- Errors leading to goal (assuming this is the column)
                "Int" -- Interceptions (assuming this is the column)
                -- Add columns for Headed Clearances, Duels if available
            FROM "defensive_player_stats" -- Corrected table name
            """
            df_defensive = pd.read_sql_query(query_defensive, conn)
            print(f"Loaded {len(df_defensive)} rows from defensive_player_stats.")
        except pd.io.sql.DatabaseError as e:
            print(f"Error loading defensive_player_stats: {e}")
            df_defensive = pd.DataFrame()  # Create empty DataFrame if loading fails

        # Load Player Possession Stats (needed for Recoveries, maybe Duels)
        try:
            query_possession = """
            SELECT
                "Player_ID", "year", "league",
                "Rec" -- Recoveries (assuming this is the column)
                -- Add columns for Duels if available
            FROM "possession_player_stats" -- Corrected table name
            """
            df_possession = pd.read_sql_query(query_possession, conn)
            print(f"Loaded {len(df_possession)} rows from possession_player_stats.")
        except pd.io.sql.DatabaseError as e:
            print(f"Error loading possession_player_stats: {e}")
            df_possession = pd.DataFrame()  # Create empty DataFrame if loading fails

        # Load Player Playing Time Stats (needed for On-Off, if scoring that)
        # try:
        #     query_playingtime = '''
        #     SELECT
        #         "Player_ID", "year", "league",
        #         "Team_Success_On_Off" -- Example On-Off stat
        #     FROM "playing_time_player_stats" -- Corrected table name
        #     '''
        #     df_playingtime = pd.read_sql_query(query_playingtime, conn)
        #     print(f"Loaded {len(df_playingtime)} rows from playing_time_player_stats.")
        # except pd.io.sql.DatabaseError as e:
        #     print(f"Error loading playing_time_player_stats: {e}")
        #     df_playingtime = pd.DataFrame()

        # --- Merge DataFrames ---
        # Merge based on Player_ID, year, and league
        # Start with the standard stats as the base
        df_merged = df_standard.copy()

        # Merge with other tables
        merge_keys = ["Player_ID", "year", "league"]
        if not df_goalkeeping.empty:
            df_merged = pd.merge(
                df_merged,
                df_goalkeeping,
                on=merge_keys,
                how="left",
                suffixes=("", "_gk"),
            )
        if not df_shooting.empty:
            df_merged = pd.merge(
                df_merged,
                df_shooting,
                on=merge_keys,
                how="left",
                suffixes=("", "_shooting"),
            )
        if not df_misc.empty:
            df_merged = pd.merge(
                df_merged, df_misc, on=merge_keys, how="left", suffixes=("", "_misc")
            )
        if not df_defensive.empty:
            df_merged = pd.merge(
                df_merged,
                df_defensive,
                on=merge_keys,
                how="left",
                suffixes=("", "_def"),
            )
        if not df_possession.empty:
            df_merged = pd.merge(
                df_merged,
                df_possession,
                on=merge_keys,
                how="left",
                suffixes=("", "_pos"),
            )
        # Add other merges here if you load more tables

        # Handle potential duplicate columns after merge if suffixes weren't used correctly
        # or if columns exist in multiple tables unexpectedly.
        df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]

        if df_merged.empty:
            print("\nNo data available after merging. Cannot calculate fantasy points.")
        else:
            print(f"\nSuccessfully merged data for {len(df_merged)} players.")
            # Ensure numeric columns are still numeric after merging
            # Use a more robust check for numeric types
            numeric_cols_to_coerce = [
                col
                for col in df_merged.columns
                if col
                not in [
                    "Player_ID",
                    "year",
                    "league",
                    "Player",
                    "Squad",
                    "Pos",
                    "Player_Link",
                ]
            ]  # Exclude known text/ID columns

            for col in numeric_cols_to_coerce:
                if col in df_merged.columns:
                    # Coerce errors will turn non-numeric values into NaN
                    df_merged[col] = pd.to_numeric(df_merged[col], errors="coerce")

            # --- Calculate Fantasy Points (Partial) ---
            # Initialize a new column for fantasy points, starting at 0
            df_merged["Fantasy_Points"] = 0.0

            # Debugging print: Check dtype of Fantasy_Points before calculations
            print(
                f"\nDEBUG: Dtype of 'Fantasy_Points' before calculations: {df_merged['Fantasy_Points'].dtype}"
            )
            # Debugging print: Check dtype of Performance_Gls
            if "Performance_Gls" in df_merged.columns:
                print(
                    f"DEBUG: Dtype of 'Performance_Gls': {df_merged['Performance_Gls'].dtype}"
                )
            if "Pos" in df_merged.columns:
                print(
                    f"DEBUG: Sample 'Pos' values: {df_merged['Pos'].unique()[:5]}"
                )  # Check sample Pos values

            # --- Apply Rules Based on Available Season Total Data ---

            # Note: Playing Time rules (up to 60 mins / 60+ mins) cannot be scored accurately
            # with season totals. Skipping these.

            # Goals by Position (+6 GK, +6 DF, +5 MF, +4 FW)
            if "Performance_Gls" in df_merged.columns and "Pos" in df_merged.columns:
                # Ensure Gls is numeric and handle potential NaNs (already done by coercion loop)
                # Ensure Pos is treated as string and handle potential NaNs in Pos
                df_merged["Pos"] = df_merged["Pos"].astype(str)  # Ensure Pos is string
                df_merged["Performance_Gls"] = df_merged["Performance_Gls"].fillna(
                    0
                )  # Ensure Gls is numeric, fill NaN with 0

                # Map positions (you might need to refine these mappings based on your data)
                # Use case-insensitive check and handle potential multiple positions
                gk_mask = df_merged["Pos"].str.contains("GK", na=False, case=False)
                df_mask = (
                    df_merged["Pos"].str.contains("DF", na=False, case=False) & ~gk_mask
                )  # DF but not GK
                mf_mask = (
                    df_merged["Pos"].str.contains("MF", na=False, case=False)
                    & ~gk_mask
                    & ~df_mask
                )  # MF but not GK/DF
                fw_mask = (
                    df_merged["Pos"].str.contains("FW", na=False, case=False)
                    & ~gk_mask
                    & ~df_mask
                    & ~mf_mask
                )  # FW but not GK/DF/MF

                df_merged.loc[gk_mask, "Fantasy_Points"] += (
                    df_merged.loc[gk_mask, "Performance_Gls"] * 6
                )
                df_merged.loc[df_mask, "Fantasy_Points"] += (
                    df_merged.loc[df_mask, "Performance_Gls"] * 6
                )
                df_merged.loc[mf_mask, "Fantasy_Points"] += (
                    df_merged.loc[mf_mask, "Performance_Gls"] * 5
                )
                df_merged.loc[fw_mask, "Fantasy_Points"] += (
                    df_merged.loc[fw_mask, "Performance_Gls"] * 4
                )
                print("Applied Goal points.")

            # Assists (+3)
            if "Performance_Ast" in df_merged.columns:
                df_merged["Performance_Ast"] = df_merged["Performance_Ast"].fillna(0)
                df_merged["Fantasy_Points"] += df_merged["Performance_Ast"] * 3
                print("Applied Assist points.")

            # Penalty Miss (-2)
            if (
                "Performance_PK" in df_merged.columns
                and "Performance_PKatt" in df_merged.columns
            ):
                df_merged["Performance_PK"] = df_merged["Performance_PK"].fillna(0)
                df_merged["Performance_PKatt"] = df_merged["Performance_PKatt"].fillna(
                    0
                )
                penalty_misses = (
                    df_merged["Performance_PKatt"] - df_merged["Performance_PK"]
                )
                df_merged["Fantasy_Points"] += penalty_misses * -2
                print("Applied Penalty Miss points.")

            # Yellow Card (-1)
            if "Performance_CrdY" in df_merged.columns:
                df_merged["Performance_CrdY"] = df_merged["Performance_CrdY"].fillna(0)
                df_merged["Fantasy_Points"] += df_merged["Performance_CrdY"] * -1
                print("Applied Yellow Card points.")

            # Red Card (-3)
            if "Performance_CrdR" in df_merged.columns:
                df_merged["Performance_CrdR"] = df_merged["Performance_CrdR"].fillna(0)
                df_merged["Fantasy_Points"] += df_merged["Performance_CrdR"] * -3
                print("Applied Red Card points.")

            # Own Goal (-2) - Requires misc stats
            if "Performance_OG" in df_merged.columns:
                df_merged["Performance_OG"] = df_merged["Performance_OG"].fillna(0)
                df_merged["Fantasy_Points"] += df_merged["Performance_OG"] * -2
                print("Applied Own Goal points.")

            # Error Leading to Goal (-1) - Requires defensive/misc stats
            if "Err" in df_merged.columns:  # Assuming column is named 'Err' or similar
                df_merged["Err"] = df_merged["Err"].fillna(0)
                df_merged["Fantasy_Points"] += df_merged["Err"] * -1
                print("Applied Error leading to Goal points.")

            # Interception (> 1 interception +1) - Requires defensive/misc stats
            # Note: Rule is "More than one interception". If data is season total, this means total > 1.
            if "Int" in df_merged.columns:  # Assuming column is named 'Int' or similar
                df_merged["Int"] = df_merged["Int"].fillna(0)
                # Apply +1 if total season interceptions > 1
                df_merged.loc[df_merged["Int"] > 1, "Fantasy_Points"] += 1
                print("Applied Interception points.")

            # Penalty Save (+5) - Requires goalkeeping stats, filter by GK
            if "Penalty_Kicks_PKsv" in df_merged.columns and "Pos" in df_merged.columns:
                df_merged["Penalty_Kicks_PKsv"] = df_merged[
                    "Penalty_Kicks_PKsv"
                ].fillna(0)
                gk_mask = df_merged["Pos"].str.contains("GK", na=False, case=False)
                df_merged.loc[gk_mask, "Fantasy_Points"] += (
                    df_merged.loc[gk_mask, "Penalty_Kicks_PKsv"] * 5
                )
                print("Applied Penalty Save points.")

            # Saves (Per 2 Saves +1) - Requires goalkeeping stats, filter by GK
            if "Performance_Saves" in df_merged.columns and "Pos" in df_merged.columns:
                df_merged["Performance_Saves"] = df_merged["Performance_Saves"].fillna(
                    0
                )
                gk_mask = df_merged["Pos"].str.contains("GK", na=False, case=False)
                # Calculate points: floor(Saves / 2) * 1
                df_merged.loc[gk_mask, "Fantasy_Points"] += (
                    np.floor(df_merged.loc[gk_mask, "Performance_Saves"] / 2) * 1
                )
                print("Applied Saves points.")

            # Goals Against (Per 2 Goals Against -1) - Requires goalkeeping stats for GK, maybe defensive for DF
            # Scoring for GK: Requires Performance_GA from goalkeeping stats
            if "Performance_GA" in df_merged.columns and "Pos" in df_merged.columns:
                df_merged["Performance_GA"] = df_merged["Performance_GA"].fillna(0)
                gk_mask = df_merged["Pos"].str.contains("GK", na=False, case=False)
                # Calculate points: floor(GA / 2) * -1
                df_merged.loc[gk_mask, "Fantasy_Points"] += (
                    np.floor(df_merged.loc[gk_mask, "Performance_GA"] / 2) * -1
                )
                print("Applied Goals Against (GK) points.")

            # Goals Against (Per 2 Goals Against -1) - Scoring for DF: Requires Goals Against data for defenders
            # This is hard with season totals. Skipping accurate DF goals against for now.
            # if "Goals_Against_as_Defender" in df_merged.columns and "Pos" in df_merged.columns:
            #      df_mask = df_merged["Pos"].str.contains("DF", na=False) & ~df_merged["Pos"].str.contains("GK", na=False)
            #      df_merged.loc[df_mask, 'Fantasy_Points'] += np.floor(df_merged.loc[df_mask, "Goals_Against_as_Defender"] / 2) * -1
            #      print("Applied Goals Against (DF) points (Approximation).")

            # Shots Off Target (Per 2 Shots Off Target -1) - Requires shooting stats
            if (
                "Standard_Sh" in df_merged.columns
                and "Standard_SoT" in df_merged.columns
            ):
                df_merged["Standard_Sh"] = df_merged["Standard_Sh"].fillna(0)
                df_merged["Standard_SoT"] = df_merged["Standard_SoT"].fillna(0)
                shots_off_target = df_merged["Standard_Sh"] - df_merged["Standard_SoT"]
                # Calculate points: floor(Shots Off Target / 2) * -1
                df_merged["Fantasy_Points"] += np.floor(shots_off_target / 2) * -1
                print("Applied Shots Off Target points.")

            # Recoveries (More than 7 recoveries +1) - Requires possession/defensive stats, filter field players
            # Note: Rule is "More than 7 recoveries". If data is season total, apply +1 if total > 7.
            if "Rec" in df_merged.columns and "Pos" in df_merged.columns:
                df_merged["Rec"] = df_merged["Rec"].fillna(0)
                field_player_mask = ~df_merged["Pos"].str.contains(
                    "GK", na=False, case=False
                )
                # Apply +1 if total season recoveries > 7 for field players
                df_merged.loc[
                    field_player_mask & (df_merged["Rec"] > 7), "Fantasy_Points"
                ] += 1
                print("Applied Recoveries points (Field Players).")

            # Note: Rules for Clean Sheets, Match Bonuses, Big Chance Created, Headed Clearances,
            # Duels Won/Lost, and accurate per-match Playing Time points require data
            # not directly available or easily calculable from season totals in standard FBref tables.
            # These are skipped in this partial calculation.

            # --- Display Top Fantasy Point Scorers ---
            print("\n--- Top 20 Fantasy Point Scorers (Partial Calculation) ---")
            top_fantasy_scorers = df_merged.sort_values(
                by="Fantasy_Points", ascending=False
            )
            # Select relevant display columns
            display_cols = [
                "Player",
                "Squad",
                "year",
                "Pos",
                "Fantasy_Points",
                "Performance_Gls",
                "Performance_Ast",
                "Performance_CrdY",
                "Performance_CrdR",
                "Playing_Time_90s",  # Useful context
                # Add other relevant columns from your data
            ]
            # Filter display columns to only include those present in the DataFrame
            display_cols_present = [
                col for col in display_cols if col in top_fantasy_scorers.columns
            ]

            print(top_fantasy_scorers[display_cols_present].head(20))

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except pd.io.sql.DatabaseError as e:
        print(f"Pandas Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the connection is closed even if errors occur
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
