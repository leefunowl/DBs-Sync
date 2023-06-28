import unittest

from create_DB_and_tables import schema_creation, drop_all_tables, engine_creation, meta_creation, meta_reflection
from config import test as config

class TestSchema(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestSchema, self).__init__(*args, **kwargs)
        source_db_engine, target_db_engine = engine_creation(config.source_db_uri, config.target_db_uri)
        meta = meta_creation()
        self.source_db_engine = source_db_engine
        self.target_db_engine = target_db_engine
        self.meta = meta

    def test_schema_deletion(self):
        # source_db_engine, target_db_engine = engine_creation(config.source_db_uri, config.target_db_uri)
        # meta = meta_creation()
        # drop_all_tables(source_db_engine, target_db_engine, meta)
        # source_db_meta, target_db_meta = meta_reflection(source_db_engine, target_db_engine)
        drop_all_tables(self.source_db_engine, self.target_db_engine, self.meta)
        source_db_meta, target_db_meta = meta_reflection(self.source_db_engine, self.target_db_engine)
        # Confirm no tables in DBs
        self.assertEqual(len(source_db_meta.tables.keys()), 0)
        self.assertEqual(len(target_db_meta.tables.keys()), 0)


    def test_schema_creation(self):
        drop_all_tables(self.source_db_engine, self.target_db_engine, self.meta)
        schema_creation(self.source_db_engine, self.target_db_engine, self.meta)

        source_db_meta, target_db_meta = meta_reflection(self.source_db_engine, self.target_db_engine)
        self.assertNotEqual(len(source_db_meta.tables.keys()), 0)
        self.assertNotEqual(len(target_db_meta.tables.keys()), 0)