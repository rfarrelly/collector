import yaml
from models.job import ScrapeJob


def load_jobs(path):
    raw = yaml.safe_load(open(path))
    return raw["output_root"], [ScrapeJob(**job) for job in raw["jobs"]]
