import yaml
import sys
from pathlib import Path
from registry.leagues import load_leagues
from registry.sources import load_sources
from registry.jobs import load_jobs
from scrapers.football_data import FootballDataScraper
from scrapers.soccerstats import SoccerStatsScraper
from scrapers.base import Storage

leagues = load_leagues("collector/config/leagues.yaml")
sources = load_sources("collector/config/sources.yaml")
output_root, jobs = load_jobs("collector/config/scraper.yaml")
storage = Storage(output_root)

SCRAPER_MAP = {
    "football-data": FootballDataScraper,
    "soccerstats": SoccerStatsScraper,
}

mappings = {}
mapping_dir = Path("collector/config/mappings")


def run_scraper(source_name: str, season: str = None):
    mapping_file = mapping_dir / f"{source_name}.yaml"
    if mapping_file.exists():
        with open(mapping_file) as f:
            mappings[source_name] = yaml.safe_load(f)
    else:
        print(f"[WARN] No mapping file found for {source_name}")
        mappings[source_name] = {}

    filtered_jobs = [j for j in jobs if j.source == source_name]

    for job in filtered_jobs:

        league = leagues.get(job.league)
        source = sources.get(job.source)

        if not league or not source:
            print(f"[ERROR] Invalid config in job: {job}")
            continue

        scraper_cls = SCRAPER_MAP.get(source.name)
        if not scraper_cls:
            print(f"[ERROR] No scraper class found for {source.name}")
            continue

        scraper = scraper_cls(source)

        external_id = mappings.get(source.name, {}).get(league.name)

        if not external_id:
            print(f"[SKIP] {league.name}: No mapping ID found for source {source.name}")
            continue

        seasons = [season] if (season and season in job.seasons) else job.seasons

        for season in seasons:
            try:
                data = scraper.fetch(league, season, external_id=external_id)

                if data.empty:
                    print(f"[INFO] No data returned for {league.name} {season}")
                    continue

                path = storage.target_path(source.name, league.name, season)
                path.parent.mkdir(parents=True, exist_ok=True)
                data.to_csv(path, index=False)
                print(f"[SUCCESS] {source.name}: {league.name} {season} -> {path}")

            except Exception as e:
                print(f"[FAIL] {source.name} {league.name}: {e}")


def main():
    if len(sys.argv) > 2:
        source = sys.argv[1]
        if source not in [s for s in sources.keys()]:
            print(f"[FAIL] Invalid source")
            return 0

    # Commands
    if len(sys.argv) == 2:
        run_scraper(source_name=source)
    elif len(sys.argv) == 3:
        run_scraper(source_name=source, season=sys.argv[2])
    else:
        print(f"[WARN] Unknown command")


if __name__ == "__main__":
    main()
