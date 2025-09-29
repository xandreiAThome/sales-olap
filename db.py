from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

load_dotenv()

database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, echo=True)
Session = sessionmaker(bind=engine)

metadata = MetaData()
metadata.reflect(bind=engine)
