"""change_order_item_id_to_bigint

Revision ID: 644814aca64f
Revises: b4976bc3c720
Create Date: 2025-10-13 21:21:37.000840

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '644814aca64f'
down_revision: Union[str, Sequence[str], None] = 'b4976bc3c720'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop foreign key constraints temporarily (if any reference Order_Item_ID)
    # Change Order_Item_ID from INT to BIGINT
    op.execute('ALTER TABLE fact_order_items MODIFY COLUMN Order_Item_ID BIGINT NOT NULL')


def downgrade() -> None:
    """Downgrade schema."""
    # Change Order_Item_ID back from BIGINT to INT
    op.execute('ALTER TABLE fact_order_items MODIFY COLUMN Order_Item_ID INT NOT NULL')
