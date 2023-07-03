from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select, delete
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd, logging, datetime, os

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

def sync_existing_tables(source_db_engine, target_db_engine, table_model):

    sql = select(table_model)
    df_source = pd.read_sql(sql = sql, con = source_db_engine)
    df_target = pd.read_sql(sql = sql, con = target_db_engine)

    df_source['type'] = 'source'
    df_target['type'] = 'target'

    df_concat = pd.concat([df_source, df_target])
    primary_key = table_model.primary_key.columns.values()[0].name # .__mapper__.primary_key[0].name
    columns_2_compare = list(df_concat.columns.copy())
    columns_2_compare.remove(primary_key)
    # after drop_duplicates, the left ones from targets (type 'target') needed to be deleted (includes deleted and updated rows)
    # the left ones from sources (type 'source') needed to be inserted (includes new and updated rows)
    df_concat.drop_duplicates(subset = columns_2_compare, keep = False, inplace = True, ignore_index = True)

    df_delete = df_concat.loc[df_concat['type'] == 'target'].drop('type', axis = 1)
    df_insert = df_concat.loc[df_concat['type'] == 'source'].drop('type', axis = 1)

    # delete rows for deleted and updated rows
    if df_delete.shape[0] > 0: # only run sql if there is actually rows to delete
        Session = sessionmaker(bind = target_db_engine)
        with Session() as session:
            # sql = delete(table_model).where(getattr(table_model, primary_key).in_([int(i) for i in df_delete[primary_key].values]))
            sql = delete(table_model).where(table_model.c[primary_key].in_([int(i) for i in df_delete[primary_key].values]))
            session.execute(sql)
            session.commit()
        print('\nThese rows are deleted from target db:\n\n', df_delete)

    # insert rows for new and updated rows
    if df_insert.shape[0] > 0:
        df_insert.to_sql(name = table_model.name, con = target_db_engine, if_exists = 'append', index = False)
        print('\nThese rows are inserted into target db:\n\n', df_insert)

def log_init(log_path):
    if os.path.isfile(log_path):
        os.remove(log_path)
    logging.basicConfig(filename=log_path, level=logging.DEBUG)
