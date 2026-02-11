from pydantic import BaseModel


class Source(BaseModel):
    name: str
    base_url: str
    file_patterns: list[str]
