from sqlalchemy import Column, Date, Integer, String, Index
from .base import Base


class Dim_Users(Base):
    __tablename__ = "dim_users"

    Users_ID = Column(Integer, primary_key=True, unique=True, nullable=False)
    Username = Column(String(50), nullable=False)
    First_Name = Column(String(40), nullable=False)
    Last_Name = Column(String(40), nullable=False)
    City = Column(String(50), nullable=False)
    Country = Column(String(100), nullable=False)
    Zipcode = Column(String(20), nullable=False)
    Gender = Column(String(6), nullable=False)

    __table_args__ = (
        Index("idx_city", "City"),
        Index("idx_country", "Country"),
    )


metadata_dim_users = Dim_Users.metadata
dim_users = Dim_Users.__table__
