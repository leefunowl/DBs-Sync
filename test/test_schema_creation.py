import unittest

from utils import *
from config import test as config

class TestSchema(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestSchema, self).__init__(*args, **kwargs)
        log_init(config.log_path)
        source_db_engine, target_db_engine = engine_creation(config.source_db_uri, config.target_db_uri)
        meta = meta_creation()
        self.source_db_engine = source_db_engine
        self.target_db_engine = target_db_engine
        self.meta = meta

        drop_all_tables(self.source_db_engine, self.target_db_engine, self.meta)
        schema_creation(self.source_db_engine, self.target_db_engine, self.meta)

    @unittest.skip("testing insert")
    def test_schema_deletion(self):
        drop_all_tables(self.source_db_engine, self.target_db_engine, self.meta)
        source_db_meta, target_db_meta = meta_reflection(self.source_db_engine, self.target_db_engine)
        self.assertEqual(len(source_db_meta.tables.keys()), 0)
        self.assertEqual(len(target_db_meta.tables.keys()), 0)
        schema_creation(self.source_db_engine, self.target_db_engine, self.meta)

    @unittest.skip("testing insert")
    def test_schema_creation(self):
        drop_all_tables(self.source_db_engine, self.target_db_engine, self.meta)
        schema_creation(self.source_db_engine, self.target_db_engine, self.meta)
        source_db_meta, target_db_meta = meta_reflection(self.source_db_engine, self.target_db_engine)
        self.assertNotEqual(len(source_db_meta.tables.keys()), 0)
        self.assertNotEqual(len(target_db_meta.tables.keys()), 0)

    def test_insert(self):
        count_target_db = insert_test_data_target_db(self.target_db_engine)
        count_source_db = insert_test_data_source_db(self.source_db_engine)
        self.assertNotEqual(count_target_db, 0)
        self.assertNotEqual(count_source_db, 0)
