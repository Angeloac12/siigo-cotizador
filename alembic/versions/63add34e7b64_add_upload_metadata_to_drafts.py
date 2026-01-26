"""add upload metadata to drafts

Revision ID: 63add34e7b64
Revises: 8e7abd11faf7
Create Date: 2025-12-18 10:19:30.789661

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '63add34e7b64'
down_revision: Union[str, Sequence[str], None] = '8e7abd11faf7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("drafts", sa.Column("original_filename", sa.Text(), nullable=True))
    op.add_column("drafts", sa.Column("stored_path", sa.Text(), nullable=True))
    pass


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("drafts", "stored_path")
    op.drop_column("drafts", "original_filename")
    pass
