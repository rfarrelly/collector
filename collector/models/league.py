from pydantic import BaseModel


class League(BaseModel):
    name: str
    code: str
    country: str
