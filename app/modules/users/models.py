from datetime import datetime
from sqlalchemy import String , DateTime, Boolean, Integer, func , BigInteger , ForeignKey
from sqlalchemy.orm import Mapped, mapped_column , relationship
from app.db.base import Base

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True,autoincrement=True)
    telegram_id: Mapped[int | None] = mapped_column(BigInteger,unique=True,nullable=True,index=True)
    username: Mapped[str | None] = mapped_column(String(50), unique=True, nullable=True)
    hashed_password: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(String(100))
    admin: Mapped[bool] = mapped_column(Boolean,default=False)
    is_active: Mapped[bool] = mapped_column(Boolean,default=True)
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                server_default=func.now(),
                                                nullable=False)
    orders: Mapped[list["Order"]] = relationship("Order", back_populates="user",cascade="all, delete-orphan")



class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id: Mapped[int] = mapped_column(primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    token_hash: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        index=True,
        nullable=False,
    )

    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    user: Mapped["User"] = relationship()