from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

from config import config

def schema_creation(source_db_uri, target_db_uri):
    source_db_engine = create_engine(source_db_uri, echo = True)
    target_db_engine = create_engine(target_db_uri, echo = True)
    meta = MetaData()

    students = Table(
      'students', meta, 
      Column('id', Integer, primary_key = True), 
      Column('name', String), 
      Column('lastname', String),
    )

    try:
        meta.create_all(source_db_engine)
        meta.create_all(target_db_engine)
    except Exception as e:
        print(e)