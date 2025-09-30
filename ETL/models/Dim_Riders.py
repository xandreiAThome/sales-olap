from sqlalchemy import Column, Integer, String
from .base import Base


class Dim_Rider(Base):
    __tablename__ = "dim_riders"

    Rider_ID = Column(Integer, primary_key=True, unique=True, nullable=False)
    First_Name = Column(String(40), nullable=False)
    Last_Name = Column(String(40), nullable=False)
    Vehicle_Type = Column(String(40), nullable=False)
    Age = Column(Integer, nullable=False)
    Gender = Column(String(6), nullable=False)
    Courier_Name = Column(String(20), nullable=False)


metadata_dim_riders = Dim_Rider.metadata
dim_riders = Dim_Rider.__table__
