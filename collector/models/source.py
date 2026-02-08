from pydantic import BaseModel


class Source(BaseModel):
    name: str
    base_url: str
    file_pattern: str
    # season_format: str
