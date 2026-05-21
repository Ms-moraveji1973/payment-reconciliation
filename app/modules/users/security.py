from datetime import datetime, timedelta, timezone
from typing import Annotated
import jwt
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jwt.exceptions import InvalidTokenError
from passlib.context import CryptContext
from pydantic.v1.schema import schema
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from .schema import TokenData, TokenResponse
from .models import User





ALGORITHM = get_settings().ALGORITHM
SECRET_KEY = get_settings().SECRET_KEY

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/register/login")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def hash_password(plain_password):
    return pwd_context.hash(plain_password)


async def authenticate_user(session,username:str,password:str):
    from .service import get_user_by_username
    user = await get_user_by_username(username,session)
    if not user:
        return False
    if not verify_password(password,user.hashed_password) :
        return False
    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encode_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encode_jwt



async def get_current_user(db,token: Annotated[str,Depends(oauth2_scheme)]) -> User | None:
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
    except InvalidToken :
        raise credentials_exception
    user = await get_user_by_username(token_data.username,db)
    if user is None:
        raise credentials_exception
    return user
