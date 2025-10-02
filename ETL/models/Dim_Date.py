from sqlalchemy import Column, Integer, Date
from .base import Base


class Dim_Date(Base):
    __tablename__ = "dim_date"

    Date_ID = Column(Integer, nullable=False, primary_key=True, unique=True)
    Date = Column(Date, nullable=False, unique=True)
    Year = Column(Integer, nullable=False)
    Month = Column(Integer, nullable=False)
    Day = Column(Integer, nullable=False)
    Quarter = Column(Integer, nullable=False)


metadata_dim_date = Dim_Date.metadata
dim_date = Dim_Date.__table__
