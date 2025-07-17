from sqlalchemy import create_engine

from config import config

engine = create_engine(config.SQLALCHEMY_DATABASE_URI)
