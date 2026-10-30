"""Manage resumable provider sync state in Alembic.

Revision ID: 0020_provider_sync_state
Revises: 0019_deal_score_policies

`app.providers.sync` previously created `provider_sync_state` at runtime, so
existing databases may already have the original five-column table. This
revision adopts that table instead of failing, keeps its rows, and adds the
columns needed to resume an interrupted multi-page sync.
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0020_provider_sync_state"
down_revision: str | Sequence[str] | None = "0019_deal_score_policies"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_sync_state (
            provider_name TEXT PRIMARY KEY,
            last_status TEXT NOT NULL,
            last_cursor TEXT,
            last_error TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # The runtime table stored naive server timestamps; the containers run in UTC.
    op.execute(
        """
        ALTER TABLE provider_sync_state
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
                USING updated_at AT TIME ZONE 'UTC',
            ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP,
            ALTER COLUMN updated_at SET NOT NULL,
            ADD COLUMN started_at TIMESTAMPTZ,
            ADD COLUMN completed_at TIMESTAMPTZ,
            ADD COLUMN pages_committed INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN records_committed INTEGER NOT NULL DEFAULT 0,
            ADD CONSTRAINT ck_provider_sync_state_status
                CHECK (last_status IN ('in_progress', 'completed', 'failed'))
        """
    )


def downgrade() -> None:
    op.drop_table("provider_sync_state")
