from pydantic import BaseModel


class Source(BaseModel):
    name: str
    base_url: str
    file_pattern: str
    league_code_field: str
    season_format: str
