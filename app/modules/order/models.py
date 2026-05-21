from sqlalchemy import (String , DateTime, Boolean, Integer, func , BigInteger,
                        Enum as sqlalchemyEnum , ForeignKey,)
from sqlalchemy.orm import Mapped, mapped_column ,relationship
from enum import Enum
from datetime import datetime

from app.db.base import Base

class OrderStatus(str,Enum):
    PENDING = "PENDING"
    REVIEWED = "REVIEWED"
    PAID = "PAID"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class Order(Base):
    __tablename__ = "orders"

    id : Mapped[int] = mapped_column(primary_key=True,autoincrement=True)
    user_id : Mapped[int] = mapped_column(ForeignKey("users.id"),nullable=False)
    status : Mapped[Enum] = mapped_column(sqlalchemyEnum(OrderStatus),default=OrderStatus.PENDING)
    amount : Mapped[int] = mapped_column(Integer,nullable=False)
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),server_default=func.now(),
                                                nullable=False)
    user: Mapped["User"] = relationship("User",back_populates="orders")