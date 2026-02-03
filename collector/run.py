from registry.leagues import load_leagues
from registry.sources import load_sources
from registry.jobs import load_jobs
from scrapers.football_data import FootballDataScraper
from scrapers.base import Storage

leagues = load_leagues("collector/config/leagues.yaml")
sources = load_sources("collector/config/sources.yaml")
output_root, jobs = load_jobs("collector/config/scraper.yaml")

storage = Storage(output_root)

SCRAPER_MAP = {
    "football-data": FootballDataScraper,
}

for job in jobs:
    league = leagues[job.league]
    source = sources[job.source]
    scraper = SCRAPER_MAP[source.name](source)

    for season in job.seasons:
        data = scraper.fetch(league, season)
        path = storage.target_path(source.name, league.name, season)
        path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(path, index=False)
