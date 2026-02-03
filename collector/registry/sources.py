import yaml
from models.source import Source


def load_sources(path):
    raw = yaml.safe_load(open(path))
    return {name: Source(name=name, **data) for name, data in raw.items()}
