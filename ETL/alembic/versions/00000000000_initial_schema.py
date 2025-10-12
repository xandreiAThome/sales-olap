"""Initial schema - create all base tables

Revision ID: 00000000000_initial
Revises: 
Create Date: 2025-10-12 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '00000000000_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all base tables."""
    
    # Create dim_riders table
    op.create_table(
        'dim_riders',
        sa.Column('Rider_ID', sa.Integer(), nullable=False),
        sa.Column('First_Name', sa.String(length=50), nullable=True),
        sa.Column('Last_Name', sa.String(length=50), nullable=True),
        sa.Column('Phone_Number', sa.String(length=20), nullable=True),
        sa.Column('City', sa.String(length=50), nullable=True),
        sa.Column('Country', sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint('Rider_ID')
    )
    
    # Create dim_products table
    op.create_table(
        'dim_products',
        sa.Column('Product_ID', sa.Integer(), nullable=False),
        sa.Column('Name', sa.String(length=100), nullable=True),
        sa.Column('Category', sa.String(length=50), nullable=True),
        sa.Column('Price', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.PrimaryKeyConstraint('Product_ID')
    )
    
    # Create dim_users table
    op.create_table(
        'dim_users',
        sa.Column('User_ID', sa.Integer(), nullable=False),
        sa.Column('First_Name', sa.String(length=50), nullable=True),
        sa.Column('Last_Name', sa.String(length=50), nullable=True),
        sa.Column('Email', sa.String(length=100), nullable=True),
        sa.Column('Phone_Number', sa.String(length=20), nullable=True),
        sa.Column('Date_of_Birth', sa.Date(), nullable=True),
        sa.Column('City', sa.String(length=50), nullable=True),
        sa.Column('Country', sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint('User_ID')
    )
    
    # Create dim_date table
    op.create_table(
        'dim_date',
        sa.Column('Delivery_Date_ID', sa.Integer(), nullable=False),
        sa.Column('Date', sa.Date(), nullable=False),
        sa.Column('Year', sa.Integer(), nullable=True),
        sa.Column('Quarter', sa.Integer(), nullable=True),
        sa.Column('Month', sa.Integer(), nullable=True),
        sa.Column('Day', sa.Integer(), nullable=True),
        sa.Column('DayOfWeek', sa.Integer(), nullable=True),
        sa.Column('WeekOfYear', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('Delivery_Date_ID')
    )
    
    # Create fact_order_items table
    op.create_table(
        'fact_order_items',
        sa.Column('Order_Item_ID', sa.Integer(), nullable=False),
        sa.Column('Order_ID', sa.Integer(), nullable=True),
        sa.Column('Product_ID', sa.Integer(), nullable=False),
        sa.Column('Delivery_Date_ID', sa.Integer(), nullable=False),
        sa.Column('Delivery_Rider_ID', sa.Integer(), nullable=True),
        sa.Column('User_ID', sa.Integer(), nullable=False),
        sa.Column('Quantity', sa.Integer(), nullable=True),
        sa.Column('Total_Revenue', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.PrimaryKeyConstraint('Order_Item_ID')
    )


def downgrade() -> None:
    """Drop all tables."""
    op.drop_table('fact_order_items')
    op.drop_table('dim_date')
    op.drop_table('dim_users')
    op.drop_table('dim_products')
    op.drop_table('dim_riders')
