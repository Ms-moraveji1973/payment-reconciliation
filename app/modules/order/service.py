from pydantic import BaseModel
from typing import Annotated


async def calculate_age(age:int):
    x = age * 10
    return x