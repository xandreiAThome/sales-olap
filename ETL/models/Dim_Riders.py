from sqlalchemy import Column, Integer, String, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from .base import Base


class DimRider(Base):
    __tablename__ = "dim_riders"

    Rider_ID = Column(Integer, primary_key=True, autoincrement=True)
    First_Name = Column(String(40), nullable=False)
    Last_Name = Column(String(40), nullable=False)
    Vehicle_Type = Column(String(40), nullable=False)
    Age = Column(Integer, nullable=False)
    Gender = Column(String(6), nullable=False)
    Courier_Name = Column(String(20), nullable=False)
