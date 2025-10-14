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
    # Use Alembic's alter_column which works across databases
    with op.batch_alter_table('fact_order_items', schema=None) as batch_op:
        batch_op.alter_column('Order_Item_ID',
                              existing_type=sa.Integer(),
                              type_=sa.BigInteger(),
                              existing_nullable=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Change Order_Item_ID back from BIGINT to INT
    with op.batch_alter_table('fact_order_items', schema=None) as batch_op:
        batch_op.alter_column('Order_Item_ID',
                              existing_type=sa.BigInteger(),
                              type_=sa.Integer(),
                              existing_nullable=False)
