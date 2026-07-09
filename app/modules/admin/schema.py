from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class UserSchema(BaseModel):
    id : int
    username : str
    name : str
    is_active : bool
    class Config:
        from_attributes = True

class PaymentIntentSchema(BaseModel):
    id: int
    status: str
    exact_amount: int
    base_amount: int
    created_at: datetime
    class Config:
        from_attributes = True

class OrderResponseSchema(BaseModel):
    id: int
    status: str
    user: UserSchema
    created_at: datetime
    amount: int
    payment: PaymentIntentSchema
    class Config:
        from_attributes = True

class PaginatedOrdersResponse(BaseModel):
    orders: List[OrderResponseSchema]
    next_cursor: Optional[int]
