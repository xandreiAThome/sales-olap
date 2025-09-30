from sqlalchemy import Column, Integer, String, Numeric
from .base import Base


class Dim_Products(Base):
    __tablename__ = "dim_products"

    Product_ID = Column(Integer, primary_key=True, unique=True, nullable=False)
    Product_Code = Column(String(20), nullable=False)
    Category = Column(String(50), nullable=False)
    Description = Column(String(255), nullable=False)
    Name = Column(String(100), nullable=False)
    Price = Column(Numeric(10, 2), nullable=False)


metadata_dim_products = Dim_Products.metadata
dim_products = Dim_Products.__table__
