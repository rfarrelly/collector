from pydantic import BaseModel
from typing import List


class ScrapeJob(BaseModel):
    source: str
    league: str
    seasons: List[str]
