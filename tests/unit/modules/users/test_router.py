"""Unit tests for Users Router error handling."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError

from app.modules.users.router import (
    create_telegram_user,
    register_user,
    login_user,
)
from app.modules.users.schema import (
    UserTelegramSchema,
    UserRegisterSchema,
)
from app.modules.users.exceptions import (
    UserAlreadyExistsError,
    UsernameAlreadyExistsError,
    RefreshTokenAlreadyExistsError,
)
from app.modules.users.models import User


@pytest.mark.asyncio
async def test_create_telegram_user_duplicate_conflict(mocker, mock_session):
    user_schema = UserTelegramSchema(telegram_id=123456, username="tele_user", name="Tele")
    mocker.patch(
        "app.modules.users.router.create_telegram_user_service",
        new_callable=AsyncMock,
        side_effect=UserAlreadyExistsError("User already exists")
    )

    with pytest.raises(HTTPException) as exc_info:
        await create_telegram_user(user=user_schema, session=mock_session)

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail == "User already exists"
    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_register_user_duplicate_username_conflict(mocker, mock_session):
    register_schema = UserRegisterSchema(username="taken_user", password="secret_password", name="Taken")
    mocker.patch(
        "app.modules.users.router.register_user_service",
        new_callable=AsyncMock,
        side_effect=UsernameAlreadyExistsError("Username already exists")
    )

    with pytest.raises(HTTPException) as exc_info:
        await register_user(user=register_schema, session=mock_session)

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail == "Username already exists"
    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_login_user_commit_integrity_error(mocker, mock_session):
    fake_user = User(id=1, username="test_login", hashed_password="hashed_pw")
    form_data = MagicMock()
    form_data.username = "test_login"
    form_data.password = "secret"

    mocker.patch("app.modules.users.router.authenticate_user", new_callable=AsyncMock, return_value=fake_user)
    mocker.patch("app.modules.users.router.create_access_token", return_value="fake_access_token")
    mocker.patch("app.modules.users.router.generate_refresh_token_string", return_value="fake_jti")
    mocker.patch("app.modules.users.router.create_refresh_token", return_value="fake_refresh_token")
    mocker.patch("app.modules.users.router.create_refresh_token_record_service", new_callable=AsyncMock, return_value=MagicMock())

    mock_session.commit.side_effect = IntegrityError("stmt", "params", Exception("Duplicate token constraint"))

    with pytest.raises(HTTPException) as exc_info:
        await login_user(form_data=form_data, session=mock_session)

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail == "Refresh token already exists"
    mock_session.rollback.assert_awaited_once()
