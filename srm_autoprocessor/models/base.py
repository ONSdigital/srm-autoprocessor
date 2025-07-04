from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

SCHEMA = "casev3"
SCHEMA_METADATA = MetaData(schema=SCHEMA)


class Base(DeclarativeBase):
    # SQL Alchemy requires DeclarativeBase to be subclassed to use it
    pass
