# --- Helper to flatten MultiIndex columns (copied from previous example) ---
import pandas as pd
from collections import Counter


def flatten_multiindex_columns(df: pd.DataFrame) -> list:
    """
    Flatten MultiIndex columns in a DataFrame and clean the column names.
    This function handles 'Unnamed' columns and ensures unique column names.
    Args:
        df (pd.DataFrame): The DataFrame with potentially MultiIndex columns.
    Returns:
        list: A list of cleaned, unique column names.
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
    seen: dict[str, int] = {}
    unique_columns = []
    for col in new_columns:
        if counts[col] > 1:
            seen[col] = seen.get(col, 0) + 1
            unique_columns.append(f"{col}_{seen[col]}")
        else:
            unique_columns.append(col)

    return unique_columns  # Return the list of cleaned, unique column names
