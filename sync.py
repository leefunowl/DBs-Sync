from sqlalchemy import create_engine, select, delete
from sqlalchemy.orm import sessionmaker
import pandas as pd

from config import config
from sync_.lsdb_model import Base_map

def sync(source_db_engine = None, target_db_engine = None, table_moble = None):

    sql = select(table_moble)
    # ds = source db; dt = target db
    ds = pd.read_sql(sql = sql, con = source_db_engine)
    dt = pd.read_sql(sql = sql, con = target_db_engine)

    ds['type'] = 's'
    dt['type'] = 't'
    df = pd.concat([ds, dt])
    pk = table_moble.primary_key.columns.values()[0].name # .__mapper__.primary_key[0].name
    # after drop_duplicates, the left ones from targets (type 't') needed to be deleted (includes deleted and updated rows)
    # the left ones from sources (type 's') needed to be inserted (includes new and updated rows)
    df.drop_duplicates(subset = df.columns[:-1], keep = False, inplace = True, ignore_index = True)

    # dd = df for delete; di = df for insert
    dd = df.loc[df['type'] == 't'].drop('type', axis = 1)
    di = df.loc[df['type'] == 's'].drop('type', axis = 1)

    # delete rows for deleted and updated rows
    if dd.shape[0] > 0: # only run sql if there is actually rows to delete
        Session = sessionmaker(bind = target_db_engine)
        with Session() as session:
            # sql = delete(table_moble).where(getattr(table_moble, pk).in_([int(i) for i in dd[pk].values]))
            sql = delete(table_moble).where(table_moble.c[pk].in_([int(i) for i in dd[pk].values]))
            session.execute(sql)
            session.commit()
        print('\nThese rows are deleted from target db:\n\n', dd)

    # insert rows for new and updated rows
    if di.shape[0] > 0:
        di.to_sql(name = table_moble.name, con = target_db_engine, if_exists = 'append', index = False)
        print('\nThese rows are inserted into target db:\n\n', di)

def main(source_db = None, target_db = None):
    for key, Base in Base_map.items():
        print(f'\nSyncing {key} DB ...\n')
        source_db_engine = create_engine(config.db_uri_map[source_db][key])
        target_db_engine = create_engine(config.db_uri_map[target_db][key])

        for table_moble in Base.metadata.sorted_tables:
            sync(source_db_engine = source_db_engine, target_db_engine = target_db_engine, table_moble = table_moble)

if __name__ == "__main__":
    main()
    print('\nDBs sync successfully :)\n')