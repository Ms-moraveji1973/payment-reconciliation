import uuid
from typing import Annotated
from fastapi import APIRouter, Depends, status, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, timezone
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import (DecodeError, InvalidSignatureError)

from .schema import (UserTelegramSchema, UserTelegramResponseSchema,
                     UserRegisterSchema, UserResponseSchema,
                     CursorResponse, CursorParams,
                     TokenResponse,
                     RefreshTokenRequest
                     )

from .service import (create_telegram_user_service,
                      register_user_service,
                      get_all_users,
                      get_user_by_username,
                      create_refresh_token_record_service,
                      validate_and_revoke_refresh_token_service,
                      get_refresh_token,
                      revoke_refresh_token,
                      revoke_family_token,
                      revoke_all_tokens
                      )

from .security import (create_access_token,
                       authenticate_user,
                       get_current_user,
                       generate_refresh_token_string,
                       create_refresh_token,
                       decode_refresh_token
                       )

from .models import User
from app.core.config import get_settings
from app.db.database import get_db


router = APIRouter(prefix="/users", tags=["users"])

@router.post("/telegram",response_model=UserTelegramResponseSchema,status_code=status.HTTP_201_CREATED)
async def create_telegram_user(user : UserTelegramSchema, session : AsyncSession = Depends(get_db)):
    try :
        new_user = await create_telegram_user_service(user,session)
        await session.commit()
        await session.refresh(new_user)
        return new_user
    except ValueError:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="User already exists")


@router.post("/register",response_model=UserResponseSchema,status_code=status.HTTP_201_CREATED)
async def register_user(user : UserRegisterSchema, session : AsyncSession = Depends(get_db)) -> UserResponseSchema :
    try :
        new_user = await register_user_service(user.username, user.password, user.name, session)
        await session.commit()
        await session.refresh(new_user)
        return user
    except ValueError :
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already exists")


@router.post("/login",response_model=TokenResponse,status_code=status.HTTP_200_OK)
async def login_user(form_data : Annotated[OAuth2PasswordRequestForm,Depends()], session:AsyncSession = Depends(get_db)):
    user = await authenticate_user(session, form_data.username, form_data.password)
    if not user :
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    a_expires_delta = timedelta(minutes=get_settings().ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username},expires_delta=a_expires_delta)

    jti = generate_refresh_token_string()
    rt_expires_delta = timedelta(days=get_settings().REFRESH_TOKEN_EXPIRE_DAYS)
    rt_expires_date = datetime.now(timezone.utc) + rt_expires_delta
    refresh_token = create_refresh_token(user.username, jti, rt_expires_delta)
    refresh_token_record = await create_refresh_token_record_service(session, user.id, jti, rt_expires_date)
    try :
        await session.commit()
        await session.refresh(refresh_token_record)
    except ValueError :
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Refresh token already exists")

    return {"access_token" : access_token, "refresh_token" : refresh_token, "token_type" : "bearer"}


@router.get("/me", response_model=UserResponseSchema,status_code=status.HTTP_200_OK)
async def read_users_me(
   current_user: Annotated[User, Depends(get_current_user)],
):
    return current_user


@router.post("/refresh",response_model=TokenResponse)
async def refresh_token(rf_token:RefreshTokenRequest, session:AsyncSession = Depends(get_db)):
    try :
        if rf_token.refresh_token is None :
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail="Refresh Token missing")
        payload =  decode_refresh_token(rf_token.refresh_token)
    except DecodeError :
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail="Invalid token in decoding refresh token")
    except InvalidSignatureError :
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail="Token signature is invalid")

    # validate type
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail="Invalid token type")

    jti : str | None = payload.get("jti")
    sub : str | None = payload.get("sub")

    if jti is None or sub is None :
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail="Invalid token in payload")
    rt_record = await validate_and_revoke_refresh_token_service(session,jti)

    if rt_record is None :
        get_rt_record = await get_refresh_token(session,jti)

        if get_rt_record is None :
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token isn't in the database")

        elif get_rt_record.is_revoked :
            await revoke_family_token(session,get_rt_record.family_id)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token is revoked")

        if get_rt_record.expires_at < datetime.now(timezone.utc):
            await revoke_refresh_token(session,jti)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token expired",
            )
    # check match current user id with refresh user_id
    user = await get_user_by_username(sub,session)
    if user is None :
        await revoke_family_token(session,rt_record.family_id)
        await session.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail="User not found")
    current_user_id,current_username = user.id, user.username
    if current_user_id != rt_record.user_id :
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail="Forbidden")

    # create new refresh token
    new_jti = generate_refresh_token_string()
    new_rt_expires_delta = timedelta(days=get_settings().REFRESH_TOKEN_EXPIRE_DAYS)
    new_rt_expires_date = datetime.now(timezone.utc) + new_rt_expires_delta
    create_new_rt_token = create_refresh_token(current_username, new_jti, new_rt_expires_delta)
    create_new_rt_record= await create_refresh_token_record_service(session, current_user_id, new_jti, new_rt_expires_date, rt_record.family_id)
    # create new access token
    new_at_expires_delta = timedelta(minutes=get_settings().ACCESS_TOKEN_EXPIRE_MINUTES)
    create_new_access_token = create_access_token(data={"sub":current_username},expires_delta=new_at_expires_delta)

    try :
        await session.commit()
        await session.refresh(create_new_rt_record)
    except ValueError :
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="Refresh Token Conflict")

    return {"access_token": create_new_access_token, "refresh_token": create_new_rt_token, "token_type": "bearer"}


@router.get("/get-users",response_model=CursorResponse[UserTelegramResponseSchema],status_code=status.HTTP_200_OK)
async def get_users(params: CursorParams = Depends(), session:AsyncSession = Depends(get_db)):
    users,next_cursor,has_more = await get_all_users(params.cursor,params.limit,session)
    return CursorResponse(
        items=users,
        next_cursor=next_cursor,
        has_more=has_more,
    )