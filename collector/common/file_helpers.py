from pathlib import Path
import pandas as pd


def combine_files(path: str):
    files = [
        str(file)
        for file in Path(path).rglob("*.csv")
        if file.is_file()
        if "combined" not in str(file)
    ]
    pd.concat([pd.read_csv(f) for f in files]).sort_values("PPI_DIFF").to_csv(
        path + "/" + "combined.csv", index=False
    )
