from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional
from datetime import datetime



class UserSchema(BaseModel):
    telegram_id: int = Field(
        ...,
        ge=100,
        le=99999999999999999999
    )
    username: Optional[str] = Field(
        None,
        min_length=2,
        max_length=100,
        examples=["ms1973"]
    )
    name: str = Field(
        ...,
        min_length=2,
        max_length=100,
        examples=["moraveji"]
    )

    @field_validator("telegram_id")
    @classmethod
    def validate_telegram_id(cls, v:int) -> int:
        if v <= 100 :
            raise ValueError("telegram_id must be greater ")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Name cannot be empty")
        return v


class UserResponseSchema(BaseModel):
    telegram_id: int
    username: Optional[str]
    name: str
    admin: Optional[bool]
    is_active: Optional[bool]
    joined_at: Optional[datetime]
    model_config = ConfigDict(from_attributes=True)



from pydantic import BaseModel, Field
from typing import Generic, TypeVar, Optional, List

T = TypeVar("T")

class CursorParams(BaseModel):
    cursor: Optional[int] = Field(None, le=20000, description=" the last cursor ")
    limit: int = Field(default=1, ge=0, le=100)

class CursorResponse(BaseModel, Generic[T]):
    items: List[T]
    next_cursor: Optional[int]
    has_more: bool