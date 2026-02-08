import pandas as pd


class FootballDataScraper:
    def __init__(self, source):
        self.source = source

    def build_url(self, external_id, season):
        # Uses external_id passed from the runner
        return (
            f"{self.source.base_url}/"
            f"{self.source.file_pattern.format(
                season=season,
                league_code=external_id
            )}"
        )

    def fetch(self, league, season, external_id):
        url = self.build_url(external_id, season)
        print(f"[{self.source.name}] Fetching {url}...")

        try:
            df = pd.read_csv(url, encoding="latin-1").copy()
        except Exception as e:
            print(f"[{self.source.name}] Failed to fetch {url}: {e}")
            return pd.DataFrame()

        df = df.assign(League=league.name, Season=season)

        # Robust date parsing
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["Date"])

        df["Week"] = df["Date"].dt.isocalendar().week
        df["Day"] = df["Date"].dt.day_name()
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

        target_cols = [
            "League",
            "Season",
            "Date",
            "Time",
            "Day",
            "Week",
            "HomeTeam",
            "AwayTeam",
            "FTHG",
            "FTAG",
            "FTR",
            "B365CH",
            "B365CD",
            "B365CA",
        ]

        # Only select columns that exist in the source
        available_cols = [c for c in target_cols if c in df.columns]
        df = df[available_cols]

        return df.sort_values("Date")
