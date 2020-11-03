import argparse
import pickle

from bigsi.constants import DEFAULT_BERKELEY_DB_STORAGE_CONFIG, DEFAULT_ROCKS_DB_STORAGE_CONFIG, \
    DEFAULT_REDIS_STORAGE_CONFIG
from bigsi.storage import get_storage


BERKELEYDB = "berkeleydb"
REDIS = "redis"
ROCKSDB = "rocksdb"


def determine_config(storage_engine, storage_filename=None):
    if storage_filename:
        if storage_engine == REDIS:
            [host, port] = storage_filename.split(':')
            storage_config = {
                "host": host, "port": port
            }
        else:
            storage_config = {"filename": storage_filename}
    else:
        if storage_engine == BERKELEYDB:
            storage_config = DEFAULT_BERKELEY_DB_STORAGE_CONFIG
        elif storage_engine == ROCKSDB:
            storage_config = DEFAULT_ROCKS_DB_STORAGE_CONFIG
        else:
            storage_config = DEFAULT_REDIS_STORAGE_CONFIG

    config = {
        "storage-engine": storage_engine,
        "storage-config": storage_config
    }

    return config


def migrate(mapping_filepath, storage_engine, storage_filename=None):
    config = determine_config(storage_engine, storage_filename)
    storage = get_storage(config)

    with open(mapping_filepath, 'r') as infile:
        mapping = pickle.load(infile)
        for isolate_id, experiment_id in mapping.items():
            storage[experiment_id] = storage[isolate_id]


parser = argparse.ArgumentParser(description='Migrate sample IDs from external IDs to Atlas tracking IDs.')
parser.add_argument('mapping_filepath', type=str, help='Path to the old-new ID mapping pickle file')
parser.add_argument('storage_engine', type=str, choices=[BERKELEYDB, ROCKSDB, REDIS], help='Storage engine')
parser.add_argument('--storage_filename', type=str, default=None, help='berkeleydb/rocksdb filename or redis host:port')

if __name__ == '__main__':
    args = parser.parse_args()
    migrate(args.mapping_filepath, args.storage_engine, args.storage_filename)