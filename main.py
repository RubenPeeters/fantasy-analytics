import ScraperFC as sfc


def main():
    print("Hello from fantasy-analytics!")
    fb = sfc.FBref()
    match = fb.scrape_match(
        "https://fbref.com/en/matches/67ed3ba2/Brentford-Tottenham-Hotspur-August-13-2023-Premier-League"
    )
    print(match.loc[0, "Away Player Stats"]["Summary"].head())


if __name__ == "__main__":
    main()
