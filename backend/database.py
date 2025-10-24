from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#This basically connects to the database we wanna access.
URL_DATABASE = 'postgresql://postgres:NewPassword!@localhost:5432/postgres'
engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()