import os

class config(object):
    source_db_uri = 'mysql+mysqldb://username:password@mydbdomain.com/SOURCE_DB'
    target_db_uri = 'mysql+mysqldb://username:password@mydbdomain.com/TARGET_DB'
    dir_path = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(dir_path, 'sche.log')
    
class test(config):
    source_db_uri = 'sqlite:///./DBs/source_db.db'
    target_db_uri = 'sqlite:///./DBs/target_db.db'