import logging

from utils import automap_classes, sync_existing_tables, engine_creation, log_init
from config import test as config

def main(source_db_engine, target_db_engine):
    Base_source_db = automap_classes(source_db_engine)
    Base_target_db = automap_classes(target_db_engine)

    for table_model in Base_source_db.metadata.sorted_tables:
        if table_model in Base_target_db.metadata.sorted_tables:
            sync_existing_tables(source_db_engine, target_db_engine, table_model)

if __name__ == "__main__":
    log_init(config.log_path)
    try:
        source_db_engine, target_db_engine = engine_creation(config.source_db_uri, config.target_db_uri)
        main(source_db_engine, target_db_engine)
        logging.info('\nDBs sync successfully :)\n')
    except Exception as e:
        logging.exception(e)