from pydantic import BaseModel, field_validator
from typing import List, Union


class ScrapeJob(BaseModel):
    source: str
    league: str
    seasons: List[Union[str, int]]

    @field_validator("seasons", mode="before")
    @classmethod
    def normalize_seasons(cls, v):
        return [str(s) for s in v]
