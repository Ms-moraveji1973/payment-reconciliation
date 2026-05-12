from datetime import datetime
from sqlalchemy import String , DateTime, Boolean, Integer, func , BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True,autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger,unique=True,nullable=False,index=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=True)
    name: Mapped[str] = mapped_column(String(100))
    admin: Mapped[bool] = mapped_column(Boolean,default=False)
    is_active: Mapped[bool] = mapped_column(Boolean,default=True)
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                server_default=func.now(),
                                                nullable=False)
