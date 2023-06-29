from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import Session

def engine_creation(source_db_uri, target_db_uri):
    source_db_engine = create_engine(source_db_uri, echo = True)
    target_db_engine = create_engine(target_db_uri, echo = True)

    return source_db_engine, target_db_engine

def meta_reflection(source_db_engine, target_db_engine):
    source_db_meta = MetaData()
    source_db_meta.reflect(bind = source_db_engine)
    target_db_meta = MetaData()
    target_db_meta.reflect(bind = target_db_engine)

    return source_db_meta, target_db_meta

def automap_classes(engine):
    Base = automap_base()
    Base.prepare(autoload_with=engine)

    return Base


def meta_creation():
    meta = MetaData()

    students = Table(
      'students', meta, 
      Column('id', Integer, primary_key = True, autoincrement=True), 
      Column('name', String), 
      Column('lastname', String),
    )

    return meta

def schema_creation(source_db_engine, target_db_engine, meta):
    try:
        meta.create_all(source_db_engine)
        meta.create_all(target_db_engine)

        return True
    except Exception as e:
        print(e)

# def drop_all_tables(source_db_engine, target_db_engine, source_db_meta, target_db_meta):
#     source_db_meta.bind = source_db_engine
#     target_db_meta.bind = target_db_engine
#     try:
#       for table in reversed(source_db_meta.sorted_tables):
#           source_db_engine.execute(table.drop())
#       for table in reversed(target_db_meta.sorted_tables):
#           target_db_engine.execute(table.drop())
#     except Exception as e:
#         print(e)

def drop_all_tables(source_db_engine, target_db_engine, meta):
    try:
        meta.drop_all(source_db_engine)
        meta.drop_all(target_db_engine)
    except Exception as e:
        print(e)

def insert_test_data_source_db(engine):
    Base = automap_classes(engine)
    students = Base.classes.students
    student1 = students(name = "leefun owl", lastname = "owl")
    student2 = students(name = "nick foles", lastname = "foles")
    student3 = students(name = "tom brady", lastname = "brady")
    
    with Session(engine) as session:
        session.add(student1)
        session.add(student2)
        session.add(student3)
        session.commit()
        
        return session.query(students).count()

def insert_test_data_target_db(engine):
    Base = automap_classes(engine)
    students = Base.classes.students
    student1 = students(name = "leefun owl", lastname = "owl")
    student2 = students(name = "foles", lastname = "foles")
    student3 = students(name = "tom", lastname = "brady")
    
    with Session(engine) as session:
        session.add(student1)
        session.add(student2)
        session.add(student3)
        session.commit()
        
        return session.query(students).count()
