from pydantic import BaseModel , Field , ConfigDict
from enum import Enum
from datetime import datetime
from .models import OrderStatus

class OrderSchema(BaseModel):
    amount : int = Field(..., gt=0,description="The amount of the order must be grater than 0 :")


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
    status : OrderStatus
    exact_amount : int
    order_id : int
    order : OrderPaymentResponse
    model_config = ConfigDict(from_attributes=True)