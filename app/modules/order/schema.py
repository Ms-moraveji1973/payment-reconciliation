from pydantic import BaseModel , Field , ConfigDict,field_validator
from enum import Enum
from datetime import datetime
from .models import OrderStatus

class OrderSchema(BaseModel):
    amount : int = Field(..., gt=0,description="The amount of the order must be grater than 0 :")
    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v:int) -> int:
        if v > 1550000 or v < 1500000 :
            raise ValueError("The amount must be grater than 1500000 and smaller than 1550000")
        return v

class PaymentIntentResponseSchema(BaseModel):
    id: int
    status: OrderStatus
    base_amount: int
    exact_amount: int
    created_at: datetime
    expired_at: datetime
    model_config = ConfigDict(from_attributes=True)

class OrderResponseSchema(BaseModel):
    id : int
    user_id : int
    amount : int
    status : OrderStatus
    created_at : datetime
    payment_intent: PaymentIntentResponseSchema | None = None
    model_config = ConfigDict(from_attributes=True)


class SmsWebhookPayload(BaseModel):
    from_number : str
    content : str
    timestamp : int


class OrderPaymentResponse(BaseModel):
    id : int
    user_id : int
    status : OrderStatus
    model_config = ConfigDict(from_attributes=True)

class SmsWebhookPayloadResponse(BaseModel):
    status : str
    redis_message_id: str
    transaction_id : int