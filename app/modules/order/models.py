
from sqlalchemy import (DateTime, Integer, func,
                        Enum as sqlalchemyEnum,
                        ForeignKey,
                        Boolean,
                        Index,
                        text
                        )
from sqlalchemy.orm import Mapped, mapped_column ,relationship
from sqlalchemy.ext.hybrid import hybrid_property
from enum import Enum
from datetime import datetime, timezone

from app.db.base import Base

class OrderStatus(str,Enum):
    PENDING = "PENDING"
    REVIEWED = "REVIEWED"
    PAID = "PAID"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class Order(Base):
    __tablename__ = "orders"

    id : Mapped[int] = mapped_column(Integer,primary_key=True,autoincrement=True)
    user_id : Mapped[int] = mapped_column(ForeignKey("users.id"),nullable=False,index=True)
    status : Mapped[Enum] = mapped_column(sqlalchemyEnum(OrderStatus),default=OrderStatus.PENDING)
    amount : Mapped[int] = mapped_column(Integer,nullable=False)
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),server_default=func.now(),
                                                nullable=False)
    user: Mapped["User"] = relationship("User",back_populates="orders")
    payment_intent: Mapped["PaymentIntent"] = relationship("PaymentIntent",back_populates='order')



class PaymentIntent(Base):
    __tablename__ = "payment_intent"
    id : Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id : Mapped[int] = mapped_column(ForeignKey("orders.id"),nullable=False,index=True)
    status : Mapped[int] = mapped_column(sqlalchemyEnum(OrderStatus))
    base_amount : Mapped[int] = mapped_column(Integer, nullable=False)
    exact_amount : Mapped[int] = mapped_column(Integer, unique=True)
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),server_default=func.now(),
                                                nullable=False)
    expired_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),server_default=text("CURRENT_TIMESTAMP + INTERVAL '1 day'"),
                                                  nullable=False)
    order : Mapped["Order"] = relationship("Order",back_populates="payment_intent")
    __table_args__ = (
                Index(
                    'idx_pending_price',
                    exact_amount,
                    unique=True,
                    postgresql_where=text("status = 'PENDING'")
                    ),
                )

    @hybrid_property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expired_at
