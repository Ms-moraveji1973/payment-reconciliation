from fastapi import APIRouter, Depends, status, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import  timedelta

from .schema import (UserTelegramSchema, UserTelegramResponseSchema,
                     UserSchema, UserResponseSchema,
                     UserRegisterResponseSchema,
                     CursorResponse, CursorParams)

from .service import (create_telegram_user_service,
                      register_user_service,
                      get_all_users)

from .security import create_access_token

from app.core.config import get_settings
from app.db.database import get_db


router = APIRouter(prefix="/users", tags=["users"])

@router.post("/telegram",response_model=UserTelegramResponseSchema,status_code=status.HTTP_201_CREATED)
async def create_telegram_user(user:UserTelegramSchema,session:AsyncSession = Depends(get_db)):
    try :
        new_user = await create_telegram_user_service(user,session)
        await session.commit()
        await session.refresh(new_user)
        return new_user
    except ValueError:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="User already exists")


@router.post("/register",response_model=UserRegisterResponseSchema,status_code=status.HTTP_201_CREATED)
async def register_user(user:UserSchema,session:AsyncSession = Depends(get_db)) -> UserRegisterResponseSchema :
    try :
        new_user = await register_user_service(user.username, user.password, user.name, session)
        expires_delta = timedelta(get_settings().ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(data={"sub": new_user.username}, expires_delta=expires_delta)
        await session.commit()
        await session.refresh(new_user)
        return {"user" : new_user, "token" : {"access_token" : access_token , "token_type" : "bearer"}}

    except ValueError :
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already exists")


@router.post("/register/login",response_model=UserResponseSchema,status_code=status.HTTP_200_OK)
async def login_user(session:AsyncSession = Depends(get_db)):
    pass


@router.get("/get-users",response_model=CursorResponse[UserTelegramResponseSchema],status_code=status.HTTP_200_OK)
async def get_users(params: CursorParams = Depends(), session:AsyncSession = Depends(get_db)):
    users,next_cursor,has_more = await get_all_users(params.cursor,params.limit,session)
    return CursorResponse(
        items=users,
        next_cursor=next_cursor,
        has_more=has_more,
    )