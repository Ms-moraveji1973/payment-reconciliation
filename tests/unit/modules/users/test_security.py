import pytest
from datetime import timedelta, datetime, timezone
import jwt
from fastapi import HTTPException
from unittest.mock import AsyncMock, MagicMock

from app.modules.users.security import (
    verify_password,
    hash_password,
    authenticate_user,
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    generate_refresh_token_string,
    get_current_user,
    SECRET_KEY,
    ALGORITHM,
)
from app.modules.users.models import User


def test_hash_and_verify_password():
    password = "SuperSecretPassword123"
    hashed = hash_password(password)

    assert hashed != password
    assert verify_password(password, hashed) is True
    assert verify_password("WrongPassword!", hashed) is False


@pytest.mark.asyncio
async def test_authenticate_user_success(mocker, mock_session):
    mock_get_user = mocker.patch("app.modules.users.service.get_user_by_username", new_callable=AsyncMock)
    fake_user = User(id=1, username="sara", hashed_password=hash_password("valid_pass"))
    mock_get_user.return_value = fake_user

    authenticated = await authenticate_user(mock_session, "sara", "valid_pass")
    assert authenticated == fake_user


@pytest.mark.asyncio
async def test_authenticate_user_not_found(mocker, mock_session):
    mock_get_user = mocker.patch("app.modules.users.service.get_user_by_username", new_callable=AsyncMock)
    mock_get_user.return_value = None

    authenticated = await authenticate_user(mock_session, "unknown_user", "password")
    assert authenticated is False


@pytest.mark.asyncio
async def test_authenticate_user_wrong_password(mocker, mock_session):
    mock_get_user = mocker.patch("app.modules.users.service.get_user_by_username", new_callable=AsyncMock)
    fake_user = User(id=1, username="ali", hashed_password=hash_password("correct_pass"))
    mock_get_user.return_value = fake_user

    authenticated = await authenticate_user(mock_session, "ali", "wrong_pass")
    assert authenticated is False


def test_create_access_token():
    token = create_access_token(data={"sub": "reza"})
    decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    assert decoded["sub"] == "reza"
    assert decoded["type"] == "access"
    assert "exp" in decoded


def test_create_access_token_custom_delta():
    custom_delta = timedelta(hours=2)
    token = create_access_token(data={"sub": "reza"}, expires_delta=custom_delta)
    decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    assert decoded["sub"] == "reza"
    assert decoded["type"] == "access"


def test_create_and_decode_refresh_token():
    token = create_refresh_token(sub="reza", jti="unique_jti_456")
    decoded = decode_refresh_token(token)

    assert decoded["sub"] == "reza"
    assert decoded["jti"] == "unique_jti_456"
    assert decoded["type"] == "refresh"


def test_generate_refresh_token_string():
    jti_1 = generate_refresh_token_string()
    jti_2 = generate_refresh_token_string()

    assert isinstance(jti_1, str)
    assert len(jti_1) > 20
    assert jti_1 != jti_2


@pytest.mark.asyncio
async def test_get_current_user_valid_access_token(mocker, mock_session):
    token = create_access_token(data={"sub": "valid_user"})
    mock_get_user = mocker.patch("app.modules.users.service.get_user_by_username", new_callable=AsyncMock)
    fake_user = User(id=10, username="valid_user")
    mock_get_user.return_value = fake_user

    user = await get_current_user(token=token, db=mock_session)
    assert user == fake_user
    mock_get_user.assert_awaited_once_with("valid_user", mock_session)


@pytest.mark.asyncio
async def test_get_current_user_rejects_refresh_token(mock_session):
    refresh_tok = create_refresh_token(sub="valid_user", jti="jti_789")

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(token=refresh_tok, db=mock_session)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Could not validate credentials"


@pytest.mark.asyncio
async def test_get_current_user_missing_sub(mock_session):
    payload = {"type": "access", "exp": datetime.now(timezone.utc) + timedelta(minutes=5)}
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(token=token, db=mock_session)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_invalid_token_string(mock_session):
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(token="not.a.valid.jwt.token", db=mock_session)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_not_in_database(mocker, mock_session):
    token = create_access_token(data={"sub": "ghost_user"})
    mock_get_user = mocker.patch("app.modules.users.service.get_user_by_username", new_callable=AsyncMock)
    mock_get_user.return_value = None

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(token=token, db=mock_session)

    assert exc_info.value.status_code == 401
