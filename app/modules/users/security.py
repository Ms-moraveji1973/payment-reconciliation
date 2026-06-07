from datetime import datetime, timedelta, timezone
from typing import Annotated
import jwt
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError
from passlib.context import CryptContext
from pydantic.v1.schema import schema
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.ext.asyncio import AsyncSession
import secrets

from app.core.config import get_settings
from app.db.database import get_db
from .schema import TokenData, TokenResponse
from .models import User


ALGORITHM = get_settings().ALGORITHM
SECRET_KEY = get_settings().SECRET_KEY

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def hash_password(plain_password):
    return pwd_context.hash(plain_password)


async def authenticate_user(session,username:str,password:str):
    from .service import get_user_by_username
    user = await get_user_by_username(username,session)
    if not user:
        return False
    if not await run_in_threadpool(verify_password, password, user.hashed_password) :
        return False
    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire,"type":"access"})
    encode_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encode_jwt


def create_refresh_token(sub:str, jti:str, expires_delta:timedelta | None = None):
    to_encode = {
        "sub": sub,
        "jti":jti,
        "type":"refresh"
    }
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(days=7)
    to_encode.update({"exp": expire,"type":"refresh"})
    encode_refresh_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encode_refresh_jwt


def decode_refresh_token(refresh_token:str):
    return jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])


def generate_refresh_token_string():
    return secrets.token_urlsafe(32)


async def get_current_user(token: Annotated[str,Depends(oauth2_scheme)],db:AsyncSession=Depends(get_db)) -> User | None:
    from .service import get_user_by_username
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try :
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username : str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except InvalidTokenError :
        raise credentials_exception
    user = await get_user_by_username(token_data.username,db)
    if user is None:
        raise credentials_exception
    return user
