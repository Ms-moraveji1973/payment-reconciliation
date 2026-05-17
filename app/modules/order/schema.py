from pydantic import BaseModel , Field , ConfigDict
from enum import Enum
from datetime import datetime
from .models import OrderStatus

class OrderSchema(BaseModel):
    telegram_id : int
    amount : float = Field(..., gt=0,description="The amount of the order must be grater than 0 :")


class OrderResponseSchema(BaseModel):
    id : int
    user_id : int
    amount : float
    status : OrderStatus
    created_at : datetime
    model_config = ConfigDict(from_attributes=True)
