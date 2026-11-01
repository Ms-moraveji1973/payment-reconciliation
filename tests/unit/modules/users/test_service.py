import pytest
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.exc import IntegrityError

from app.modules.users.service import (
    create_telegram_user_service,
    get_user_by_telegram_id,
    register_user_service,
    get_user_by_username,
    get_all_users,
    create_refresh_token_record_service,
    validate_and_revoke_refresh_token_service,
    get_refresh_token,
    revoke_refresh_token,
    revoke_family_token,
    revoke_all_tokens,
)
from app.modules.users.schema import UserTelegramSchema
from app.modules.users.models import User, RefreshToken
from app.modules.users.exceptions import (
    UserAlreadyExistsError,
    UsernameAlreadyExistsError,
    TokenRevocationError,
)


@pytest.mark.asyncio
async def test_create_telegram_user_success(mock_session):
    user_data = UserTelegramSchema(telegram_id=123456789, username="telegram_user", name="Ali")
    user = await create_telegram_user_service(user_data, mock_session)

    mock_session.add.assert_called_once()
    mock_session.flush.assert_awaited_once()
    assert user.telegram_id == 123456789
    assert user.username == "telegram_user"
    assert user.name == "Ali"


@pytest.mark.asyncio
async def test_create_telegram_user_duplicate_error(mock_session):
    user_data = UserTelegramSchema(telegram_id=123456789, username="telegram_user", name="Ali")
    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("Duplicate telegram_id"))

    with pytest.raises(UserAlreadyExistsError, match="User already exists"):
        await create_telegram_user_service(user_data, mock_session)


@pytest.mark.asyncio
async def test_get_user_by_telegram_id(mock_session):
    fake_user = User(id=1, telegram_id=987654321, name="Reza")
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_user
    mock_session.execute.return_value = mock_result

    user = await get_user_by_telegram_id(987654321, mock_session)
    assert user == fake_user
    mock_session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_user_by_telegram_id_not_found(mock_session):
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result

    user = await get_user_by_telegram_id(99999, mock_session)
    assert user is None


@pytest.mark.asyncio
async def test_register_user_service_success(mock_session):
    new_user = await register_user_service("new_user", "plain_password_123", "New Name", mock_session)

    mock_session.add.assert_called_once()
    mock_session.flush.assert_awaited_once()
    assert new_user.username == "new_user"
    assert new_user.name == "New Name"
    assert new_user.hashed_password != "plain_password_123"


@pytest.mark.asyncio
async def test_register_user_service_duplicate_username(mock_session):
    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("Duplicate username"))

    with pytest.raises(UsernameAlreadyExistsError, match="username already exists"):
        await register_user_service("existing_user", "password123", "User Name", mock_session)


@pytest.mark.asyncio
async def test_get_user_by_username(mock_session):
    fake_user = User(id=5, username="john_doe", name="John")
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_user
    mock_session.execute.return_value = mock_result

    user = await get_user_by_username("john_doe", mock_session)
    assert user == fake_user


@pytest.mark.asyncio
async def test_get_all_users_pagination_without_cursor(mock_session):
    users_list = [User(id=1, name="U1"), User(id=2, name="U2"), User(id=3, name="U3")]
    mock_result = MagicMock()
    # limit is 2, but query asks for limit + 1 = 3 to see if has_more is True
    mock_result.scalars.return_value.all.return_value = users_list
    mock_session.execute.return_value = mock_result

    users, next_cursor, has_more = await get_all_users(cursor=None, limit=2, session=mock_session)

    assert len(users) == 2
    assert has_more is True
    assert next_cursor == 2


@pytest.mark.asyncio
async def test_get_all_users_pagination_with_cursor_no_more(mock_session):
    users_list = [User(id=5, name="U5")]
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = users_list
    mock_session.execute.return_value = mock_result

    users, next_cursor, has_more = await get_all_users(cursor=4, limit=2, session=mock_session)

    assert len(users) == 1
    assert has_more is False
    assert next_cursor is None


@pytest.mark.asyncio
async def test_create_refresh_token_record_service_success(mock_session):
    expires_at = datetime.now(timezone.utc) + timedelta(days=7)
    custom_family_id = uuid.uuid4()

    token_record = await create_refresh_token_record_service(
        mock_session,
        user_id=1,
        jti="jti_abc_123",
        expired_at=expires_at,
        family_id=custom_family_id,
    )

    mock_session.add.assert_called_once()
    mock_session.flush.assert_awaited_once()
    assert token_record.user_id == 1
    assert token_record.token_hash == "jti_abc_123"
    assert token_record.family_id == custom_family_id


@pytest.mark.asyncio
async def test_create_refresh_token_record_service_duplicate_returns_none(mock_session):
    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("Duplicate token_hash"))

    token_record = await create_refresh_token_record_service(
        mock_session,
        user_id=1,
        jti="duplicate_jti",
        expired_at=datetime.now(timezone.utc),
    )
    assert token_record is None


@pytest.mark.asyncio
async def test_validate_and_revoke_refresh_token_service(mock_session):
    revoked_token = RefreshToken(id=1, token_hash="valid_jti", is_revoked=True)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = revoked_token
    mock_session.execute.return_value = mock_result

    result = await validate_and_revoke_refresh_token_service(mock_session, "valid_jti")
    assert result == revoked_token


@pytest.mark.asyncio
async def test_get_refresh_token(mock_session):
    token = RefreshToken(id=2, token_hash="sample_jti")
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = token
    mock_session.execute.return_value = mock_result

    result = await get_refresh_token(mock_session, "sample_jti")
    assert result == token


@pytest.mark.asyncio
async def test_revoke_refresh_token_success_and_error(mock_session):
    mock_session.execute.return_value = MagicMock()
    await revoke_refresh_token(mock_session, "jti_to_revoke")
    mock_session.flush.assert_awaited_once()

    # Integrity error scenario
    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("DB err"))
    with pytest.raises(TokenRevocationError, match="Error at revoking refresh token"):
        await revoke_refresh_token(mock_session, "jti_to_revoke")


@pytest.mark.asyncio
async def test_revoke_family_token_success_and_error(mock_session):
    mock_session.flush.side_effect = None
    fam_id = uuid.uuid4()
    await revoke_family_token(mock_session, fam_id)
    mock_session.flush.assert_awaited_once()

    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("DB err"))
    with pytest.raises(TokenRevocationError, match="Error at revoking family_id refresh token"):
        await revoke_family_token(mock_session, fam_id)


@pytest.mark.asyncio
async def test_revoke_all_tokens_success_and_error(mock_session):
    mock_session.flush.side_effect = None
    await revoke_all_tokens(mock_session, user_id=12)
    mock_session.flush.assert_awaited_once()

    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("DB err"))
    with pytest.raises(TokenRevocationError, match="Error at deleting all refresh tokens"):
        await revoke_all_tokens(mock_session, user_id=12)
