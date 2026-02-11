import time
import random
import pandas as pd
from datetime import datetime, timedelta
from curl_cffi import requests


class SoccerStatsScraper:
    def __init__(self, source):
        self.source = source
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        self.alt_indices = [
            "greece",
            "scotland",
            "scotland3",
            "scotland4",
            "switzerland",
            "czechrepublic",
            "denmark",
            "finland",
            "poland",
            "ukraine",
        ]

    def build_url(self, league_id, pattern_id):
        return (
            f"{self.source.base_url}/"
            f"{self.source.file_patterns[pattern_id].format(
                league_id=league_id
            )}"
        )

    def _sleep(self):
        delay = random.randint(5, 10)
        time.sleep(delay)

    def get_ppi_tables(self, league_id):
        url = self.build_url(league_id=league_id, pattern_id=1)
        # url = f"{self.source.base_url}/table.asp?league={league_id}&tid=rp"
        res = requests.get(url, headers=self.headers, impersonate="safari_ios")

        df = pd.read_html(res.content)[11].drop(
            ["Unnamed: 0", "Points Performance Index (Team PPG x Opponents PPG)"],
            axis=1,
        )
        df = df.rename(
            columns={
                "Unnamed: 1": "Team",
                "Team PPG": "TeamPPG",
                "Opponents PPG": "OppsPPG",
                "Points Performance Index (Team PPG x Opponents PPG).1": "PPI",
            }
        )
        df = df.head(len(df) - 1)
        df["OppsPPG"] = (
            df["OppsPPG"].str.extract(r"^\s*([0-9]+(?:\.[0-9]+)?)").astype(float)
        )
        df["TeamPPG"] = df["TeamPPG"].astype(float)
        df["PPI"] = df["PPI"].astype(float)

        avg_ppg = df["TeamPPG"].mean()
        df["PPINorm"] = round(df["PPI"] / avg_ppg**2, 2)
        self._sleep()
        return df

    def get_fixtures(self, league_id):
        # url = f"{self.source.base_url}/results.asp?league={league_id}"
        url = self.build_url(league_id=league_id, pattern_id=0)
        print(f"[{self.source.name}] Fetching {url}...")

        res = requests.get(url, headers=self.headers, impersonate="safari_ios")
        idx = 9 if league_id in self.alt_indices else 10

        df = pd.read_html(res.content)[idx].dropna(how="all").iloc[1:]
        df = df.rename(columns={0: "Date", 1: "Home", 2: "Time", 3: "Away"})[
            ["Date", "Home", "Time", "Away"]
        ]

        valid_time = df["Time"].str.match(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
        df = df[valid_time]

        df["Date"] = pd.to_datetime(
            df["Date"] + f" {datetime.now().year}", format="%a %d %b %Y"
        )

        self._sleep()

        today = datetime.now().date()
        mask = (df["Date"].dt.date >= today) & (
            df["Date"].dt.date <= (today + timedelta(days=3))
        )
        return df.loc[mask]

    def fetch(self, league, season, external_id):
        fixtures = self.get_fixtures(external_id)
        ppi = self.get_ppi_tables(external_id)

        combined = fixtures.merge(ppi, left_on="Home", right_on="Team")
        combined = combined.merge(
            ppi, left_on="Away", right_on="Team", suffixes=("_h", "_a")
        )

        combined["League"] = league.name
        combined["Season"] = season
        cols = [
            "League",
            "Date",
            "Time",
            "Home",
            "Away",
            "GP_h",
            "GP_a",
            "PPI_h",
            "PPI_a",
            "PPINorm_h",
            "PPINorm_a",
        ]

        combined = combined[cols]
        combined["PPI_DIFF"] = round(abs(combined["PPI_h"] - combined["PPI_a"]), 2)
        return combined.reset_index(drop=True).sort_values("PPI_DIFF")
