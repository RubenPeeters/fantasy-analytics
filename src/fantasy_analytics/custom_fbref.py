import ScraperFC as sfc
import pandas as pd
from bs4 import BeautifulSoup


class CustomFBRef(sfc.FBref):
    """
    Custom class to scrape FBRef data.
    """

    def __init__(self, *args: tuple, **kwargs: tuple) -> None:
        super().__init__(*args, **kwargs)

    def scrape_match_lineups(self, link: str) -> dict:
        r = self._get(link)
        soup = BeautifulSoup(r.content, "html.parser")
        all_tables = soup.find_all("table", class_="stats_table")
        tables_dict = {}
        print(f"Found {len(all_tables)} table elements on the page.")
        for index, lineup_table in enumerate(all_tables):
            caption_tag = lineup_table.find("caption")
            if caption_tag:
                key = caption_tag.get_text(strip=True)
                key = key.split("[")[0].strip()
            else:
                key = f"Unnamed Table {index}"
            data = []
            headers = []
            if lineup_table:
                tbody = lineup_table.find("tbody")
                if lineup_table:
                    header_row = lineup_table.find("thead").find_all("tr")[
                        1
                    ]  # Get the second header row
                    for th in header_row.find_all("th"):
                        header_name = th.get("data-stat")
                        if header_name:  # Make sure the attribute exists
                            headers.append(header_name)

                if tbody:
                    rows = tbody.find_all("tr")
                    print(f"Found {len(rows)} rows in the lineup table.")
                    for row in rows:
                        cols = row.find_all(["th", "td"])
                        row_data = [
                            ele.get_text(strip=True).replace("\xa0\xa0\xa0", "")
                            for ele in cols
                        ]  # Remove non-breaking spaces sometimes used for indenting subs
                        if len(row_data) == len(headers):
                            data.append(row_data)
                        else:
                            print(
                                f"Skipping row with incorrect column count: {row_data}"
                            )  # For debugging

            if headers and data:
                df = pd.DataFrame(data, columns=headers)
                print(df.head())
            else:
                print("Could not find table, headers, or data.")
                rows = []
                df = pd.DataFrame()

            tables_dict[key] = df
        return tables_dict
