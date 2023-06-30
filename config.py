import os

class config(object):
    source_db_uri = 'sqlite:///./DBs/source_db.db'
    target_db_uri = 'sqlite:///./DBs/target_db.db'
    dir_path = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(dir_path, 'sche.log')
class test(config):
    None