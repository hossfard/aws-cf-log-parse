import os, gzip
import cf_accesslog as AL
from cf_datastore import DataStoreBase
from cf_accesslog import AccessLog
from datetime import datetime



class DataStoreLocal(DataStoreBase):
    '''GZipped local data store, accessible by date in YYYY-mm-dd format

    '''
    def __init__(self, db_root_dir : str):
        self.db_dir = db_root_dir


    # no need to test
    def access_log(self, key : str):
        '''Keys must dates in YYYY-MM-DD format

        @param TODO
        @return
        '''
        p = self.item_path_from_date(key)
        fd = gzip.open(p, 'r')
        fd = gzip.open(key, 'r')
        return AccessLog.load(fd)


    # tested
    def item_key(self, row : list):
        date_str = row[0]
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        basedir = os.path.join(self.db_dir,
                               str(dt.year),
                               '{:02}'.format(dt.month))
        return os.path.join(basedir, '{}.gz'.format(date_str))


    # No need to test
    def overwrite(self, key : str, log : AccessLog):
        '''Overwrite existing data associated with `key

        @param {str} key key used to locate record-set
        @param {AccessLog} log accesslog to overwrite existing content

        '''
        dirname = os.path.dirname(key)
        os.makedirs(dirname, exist_ok=True)
        fd = gzip.open(key, 'wb')
        log.dump(fd)


    # untested
    def delete(self, key):
        # TODO
        pass
