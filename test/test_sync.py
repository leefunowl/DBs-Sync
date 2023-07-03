import unittest, pandas as pd

from utils import *
from config import test as config

class TestSync(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestSync, self).__init__(*args, **kwargs)
        log_init(config.log_path)
        source_db_engine, target_db_engine = engine_creation(config.source_db_uri, config.target_db_uri)
        self.source_db_engine = source_db_engine
        self.target_db_engine = target_db_engine
        meta = meta_creation()
        self.meta = meta

    def test_case_4_same_schema(self):
        drop_all_tables(self.source_db_engine, self.target_db_engine, self.meta)
        schema_creation(self.source_db_engine, self.target_db_engine, self.meta)
        insert_test_data_target_db(self.target_db_engine)
        insert_test_data_source_db(self.source_db_engine)
        Base_source_db = automap_classes(self.source_db_engine)
        students_model = Base_source_db.metadata.tables['students']
        primary_key = students_model.primary_key.columns.values()[0].name # .__mapper__.primary_key[0].name

        sync_existing_tables(self.source_db_engine, self.target_db_engine, students_model)

        sql = select(students_model)
        df_source = pd.read_sql(sql = sql, con = self.source_db_engine)
        df_target = pd.read_sql(sql = sql, con = self.target_db_engine)

        df_concat = pd.concat([df_source, df_target])
        columns_2_compare = list(df_concat.columns.copy())
        columns_2_compare.remove(primary_key)
        df_concat.drop_duplicates(subset = columns_2_compare, keep = False, inplace = True, ignore_index = True)

        self.assertEqual(df_concat.shape[0], 0)