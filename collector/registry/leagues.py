import yaml
from models.league import League


def load_leagues(path):
    raw = yaml.safe_load(open(path))
    return {name: League(name=name, **data) for name, data in raw.items()}
