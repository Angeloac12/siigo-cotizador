"""init schema

Revision ID: ac73a4e78860
Revises: 
Create Date: 2025-12-17 16:33:19.158298

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'ac73a4e78860'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "drafts",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("source_filename", sa.Text(), nullable=True),
        sa.Column("source_type", sa.Text(), nullable=True),
        sa.Column("client_document_type", sa.Text(), nullable=True),
        sa.Column("client_document_number", sa.Text(), nullable=True),
        sa.Column("upstream_client_id", sa.Text(), nullable=True),
        sa.Column("warnings_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.create_table(
        "draft_items",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("draft_id", sa.Text(), sa.ForeignKey("drafts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("line_index", sa.Integer(), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Numeric(), nullable=False),
        sa.Column("uom", sa.Text(), nullable=False),
        sa.Column("uom_raw", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Numeric(), nullable=True),
        sa.Column("warnings_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_unique_constraint("uq_draft_items_draft_line", "draft_items", ["draft_id", "line_index"])

    op.create_table(
        "client_cache",
        sa.Column("document_key", sa.Text(), primary_key=True),
        sa.Column("upstream_client_id", sa.Text(), nullable=False),
        sa.Column("cached_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("raw_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.create_table(
        "idempotency",
        sa.Column("idempotency_key", sa.Text(), primary_key=True),
        sa.Column("draft_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("quote_id", sa.Text(), nullable=True),
        sa.Column("quote_number", sa.Text(), nullable=True),
        sa.Column("response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("correlation_id", sa.Text(), nullable=True),
    )

    op.create_table(
        "request_logs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("method", sa.Text(), nullable=True),
        sa.Column("path", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("client_document_key", sa.Text(), nullable=True),
        sa.Column("draft_id", sa.Text(), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=True),
    )

def downgrade():
    op.drop_table("request_logs")
    op.drop_table("idempotency")
    op.drop_table("client_cache")
    op.drop_constraint("uq_draft_items_draft_line", "draft_items", type_="unique")
    op.drop_table("draft_items")
    op.drop_table("drafts")
