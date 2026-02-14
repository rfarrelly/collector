import pandas as pd
import asyncio
from pydoll.browser import Chrome


class FbrefScraper:
    def __init__(self, source):
        self.source = source

    def build_url(self, external_id, season):
        return (
            f"{self.source.base_url}/"
            f"{self.source.file_patterns[0].format(
                season=season,
                league_code=external_id
            )}"
        )

    async def get_data_async(self, browser: Chrome, url: str):
        # Step 1: Open a new tab
        tab = await browser.start()
        try:
            # Step 2: Load the page (and possibly bypass CAPTCHA)
            async with tab.expect_and_bypass_cloudflare_captcha(time_before_click=5):
                # Step 3: Allow page scripts / Cloudflare completion
                await asyncio.sleep(2)
                await tab.go_to(url)
                print(f"✅ Loaded: {url}")
                # Step 4: Get page data (HTTP request or HTML)
            data = await tab.request.get(url)
            return data
        except Exception as e:
            print(f"⚠️ Error fetching {url}: {e}")
            raise
        finally:
            await tab.close()

    async def fetch(self, league, season, external_id, browser):

        url = self.build_url(external_id, season)
        print(f"[{self.source.name}] Fetching {url}...")

        try:
            response = await self.get_data_async(browser, url)
        except Exception as e:
            print(e)

        columns = [
            "Wk",
            "League",
            "Season",
            "Day",
            "Date",
            "Home",
            "Score",
            "Away",
        ]

        data_df = pd.read_html(response.content)[0]

        data_df = data_df[
            ~data_df["Notes"].isin(["Match Suspended", "Match Cancelled"])
        ]

        if "Round" in data_df.columns:
            data_df = self._drop_non_regular_matches(data_df)

        data_df["League"] = external_id
        data_df["Season"] = season

        played_fixtures_df = data_df.dropna(
            how="any", subset=["Wk", "Score"], axis="index"
        )[columns]

        played_fixtures_df[["FTHG", "FTAG"]] = played_fixtures_df["Score"].apply(
            lambda x: pd.Series(self._parse_score(x))
        )
        played_fixtures_df = played_fixtures_df.drop("Score", axis="columns")

    @staticmethod
    def _parse_score(score: str) -> tuple:
        if score:
            goals = score.split("–")
            return int(goals[0]), int(goals[1])
        return 0, 0

    @staticmethod
    def _drop_non_regular_matches(df: pd.DataFrame):
        df = df.copy()
        # Identify the first value we want to keep
        first_value = df["Round"].iloc[0]

        # Keep rows until the first non-matching one
        return df[df["Round"] == first_value]
