from pathlib import Path


class Storage:
    def __init__(self, root):
        self.root = Path(root)

    def target_path(self, source, league, season):
        return self.root / source / league / f"{league}_{season}.csv"
