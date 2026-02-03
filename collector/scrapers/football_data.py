import pandas as pd


class FootballDataScraper:
    def __init__(self, source):
        self.source = source

    def build_url(self, league, season):
        league_code = getattr(league, self.source.league_code_field)

        return (
            f"{self.source.base_url}/"
            f"{self.source.file_pattern.format(
                season=season,
                league_code=league_code
            )}"
        )

    def fetch(self, league, season):
        url = self.build_url(league, season)
        df = pd.read_csv(url, encoding="latin-1").copy()
        df = df.assign(League=league.name, Season=season)

        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
        df["Week"] = df["Date"].dt.isocalendar().week
        df["Day"] = df["Date"].dt.day_name()
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

        df = df[
            [
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
        ]

        return df.sort_values("Date")
