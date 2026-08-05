"""Add input fingerprints for independently changing score components.

Revision ID: 0010_deal_component_fingerprints
Revises: 0009_comparable_valuation_scores
Create Date: 2026-08-04
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0010_deal_component_fingerprints"
down_revision: str | Sequence[str] | None = "0009_comparable_valuation_scores"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "deal_score_v2_components",
        sa.Column("input_fingerprint", sa.String(length=64)),
    )
    op.execute(
        """
        UPDATE deal_score_v2_components
        SET input_fingerprint =
            MD5(calculation_json::text)
            || MD5(calculation_json::text || id::text)
        """
    )
    op.alter_column(
        "deal_score_v2_components",
        "input_fingerprint",
        nullable=False,
    )
    op.drop_constraint(
        "uq_deal_score_v2_components_valuation",
        "deal_score_v2_components",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_deal_score_v2_components_input",
        "deal_score_v2_components",
        [
            "valuation_id",
            "scoring_version",
            "component_key",
            "input_fingerprint",
        ],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_deal_score_v2_components_input",
        "deal_score_v2_components",
        type_="unique",
    )
    op.execute(
        """
        DELETE FROM deal_score_v2_components newer
        USING deal_score_v2_components older
        WHERE newer.valuation_id = older.valuation_id
          AND newer.scoring_version = older.scoring_version
          AND newer.component_key = older.component_key
          AND newer.id > older.id
        """
    )
    op.create_unique_constraint(
        "uq_deal_score_v2_components_valuation",
        "deal_score_v2_components",
        ["valuation_id", "scoring_version", "component_key"],
    )
    op.drop_column("deal_score_v2_components", "input_fingerprint")
