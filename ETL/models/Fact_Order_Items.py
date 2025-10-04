from sqlalchemy import Column, ForeignKey, Integer, Numeric, String, null
from .base import Base


class Fact_Order_Items(Base):
    __tablename__ = "fact_order_items"

    Order_Item_ID = Column(
        Integer, primary_key=True, unique=True, nullable=False, autoincrement=True
    )
    Product_ID = Column(Integer, ForeignKey("dim_products.Product_ID"), nullable=False)
    Quantity = Column(Integer, nullable=False)
    Notes = Column(String(100), nullable=True)
    Delivery_Date_ID = Column(Integer, ForeignKey("dim_date.Date_ID"), nullable=False)
    Delivery_Rider_ID = Column(
        Integer, ForeignKey("dim_riders.Rider_ID"), nullable=False
    )
    User_ID = Column(Integer, ForeignKey("dim_users.Users_ID"), nullable=False)
    Order_Num = Column(String(20), nullable=False)
    Total_Revenue = Column(Numeric(10, 2), nullable=False)


metadata_dim_riders = Fact_Order_Items.metadata
dim_riders = Fact_Order_Items.__table__
