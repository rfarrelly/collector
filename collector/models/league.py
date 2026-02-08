from pydantic import BaseModel


class League(BaseModel):
    name: str
    country: str
