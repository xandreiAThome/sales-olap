from sqlalchemy import Column, Date, Integer, String, Index
from .base import Base


class Dim_Users(Base):
    __tablename__ = "dim_users"

    Users_ID = Column(Integer, primary_key=True, unique=True, nullable=False)
    Username = Column(String(50), nullable=False)
    First_Name = Column(String(40), nullable=False)
    Last_Name = Column(String(40), nullable=False)
    Address_1 = Column(String(100), nullable=False)
    Address_2 = Column(String(100), nullable=True)
    City = Column(String(50), nullable=False)
    Country = Column(String(100), nullable=False)
    Zipcode = Column(String(20), nullable=False)
    Phone_Number = Column(String(15), nullable=False)
    Birth_Date = Column(Date, nullable=False)
    Gender = Column(String(6), nullable=False)

    __table_args__ = (
        Index("idx_city", "City"),
        Index("idx_country", "Country"),
    )


metadata_dim_users = Dim_Users.metadata
dim_users = Dim_Users.__table__
