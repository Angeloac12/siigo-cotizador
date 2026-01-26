"""init schema

Revision ID: 8e7abd11faf7
Revises: ac73a4e78860
Create Date: 2025-12-17 16:37:23.742717

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8e7abd11faf7'
down_revision: Union[str, Sequence[str], None] = 'ac73a4e78860'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
