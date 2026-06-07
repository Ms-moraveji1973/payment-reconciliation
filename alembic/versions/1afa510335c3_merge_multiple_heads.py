"""merge multiple heads

Revision ID: 1afa510335c3
Revises: 409ca9bc0f2a, e7b9bf2b2dec
Create Date: 2026-06-07 20:00:48.193664

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1afa510335c3'
down_revision: Union[str, Sequence[str], None] = ('409ca9bc0f2a', 'e7b9bf2b2dec')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
