from create_DB_and_tables import schema_creation, drop_all_tables, engine_creation, meta_creation, meta_reflection
from config import test as config

def test_schema_deletion():
    source_db_engine, target_db_engine = engine_creation(config.source_db_uri, config.target_db_uri)
    meta = meta_creation()
    drop_all_tables(source_db_engine, target_db_engine, meta)
    source_db_meta, target_db_meta = meta_reflection(source_db_engine, target_db_engine)
    pass

test_schema_deletion()