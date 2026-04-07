"""Initial migration

Revision ID: c28c834dee85
Revises:
Create Date: 2026-02-19 13:08:14.722715

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "c28c834dee85"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
