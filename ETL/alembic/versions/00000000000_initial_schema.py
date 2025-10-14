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
        sa.Column('First_Name', sa.String(length=40), nullable=False),
        sa.Column('Last_Name', sa.String(length=40), nullable=False),
        sa.Column('Vehicle_Type', sa.String(length=40), nullable=False),
        sa.Column('Age', sa.Integer(), nullable=False),
        sa.Column('Gender', sa.String(length=6), nullable=False),
        sa.Column('Courier_Name', sa.String(length=20), nullable=False),
        sa.PrimaryKeyConstraint('Rider_ID'),
        sa.UniqueConstraint('Rider_ID')
    )
    
    # Create dim_products table
    op.create_table(
        'dim_products',
        sa.Column('Product_ID', sa.Integer(), nullable=False),
        sa.Column('Product_Code', sa.String(length=20), nullable=False),
        sa.Column('Category', sa.String(length=50), nullable=False),
        sa.Column('Description', sa.String(length=255), nullable=False),
        sa.Column('Name', sa.String(length=100), nullable=False),
        sa.Column('Price', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.PrimaryKeyConstraint('Product_ID'),
        sa.UniqueConstraint('Product_ID')
    )
    
    # Create dim_users table
    op.create_table(
        'dim_users',
        sa.Column('Users_ID', sa.Integer(), nullable=False),
        sa.Column('Username', sa.String(length=50), nullable=False),
        sa.Column('First_Name', sa.String(length=40), nullable=False),
        sa.Column('Last_Name', sa.String(length=40), nullable=False),
        sa.Column('City', sa.String(length=50), nullable=False),
        sa.Column('Country', sa.String(length=100), nullable=False),
        sa.Column('Zipcode', sa.String(length=20), nullable=False),
        sa.Column('Gender', sa.String(length=6), nullable=False),
        sa.PrimaryKeyConstraint('Users_ID'),
        sa.UniqueConstraint('Users_ID')
    )
    
    # Create dim_date table
    op.create_table(
        'dim_date',
        sa.Column('Date_ID', sa.Integer(), nullable=False),
        sa.Column('Date', sa.Date(), nullable=False),
        sa.Column('Year', sa.Integer(), nullable=False),
        sa.Column('Month', sa.Integer(), nullable=False),
        sa.Column('Day', sa.Integer(), nullable=False),
        sa.Column('Quarter', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('Date_ID'),
        sa.UniqueConstraint('Date_ID'),
        sa.UniqueConstraint('Date')
    )
    
    # Create fact_order_items table
    op.create_table(
        'fact_order_items',
        sa.Column('Order_Item_ID', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('Product_ID', sa.Integer(), nullable=False),
        sa.Column('Quantity', sa.Integer(), nullable=False),
        sa.Column('Notes', sa.String(length=100), nullable=True),
        sa.Column('Delivery_Date_ID', sa.Integer(), nullable=False),
        sa.Column('Delivery_Rider_ID', sa.Integer(), nullable=False),
        sa.Column('User_ID', sa.Integer(), nullable=False),
        sa.Column('Order_Num', sa.String(length=20), nullable=False),
        sa.Column('Total_Revenue', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.PrimaryKeyConstraint('Order_Item_ID'),
        sa.UniqueConstraint('Order_Item_ID'),
        sa.ForeignKeyConstraint(['Product_ID'], ['dim_products.Product_ID']),
        sa.ForeignKeyConstraint(['Delivery_Date_ID'], ['dim_date.Date_ID']),
        sa.ForeignKeyConstraint(['Delivery_Rider_ID'], ['dim_riders.Rider_ID']),
        sa.ForeignKeyConstraint(['User_ID'], ['dim_users.Users_ID'])
    )


def downgrade() -> None:
    """Drop all tables."""
    op.drop_table('fact_order_items')
    op.drop_table('dim_date')
    op.drop_table('dim_users')
    op.drop_table('dim_products')
    op.drop_table('dim_riders')
