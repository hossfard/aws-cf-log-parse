import os, gzip, glob, re
from .cf_datastore import DataStoreBase
from .cf_accesslog import AccessLog
from datetime import datetime



class DataStoreLocal(DataStoreBase):
    '''GZipped local data store, accessible by date in YYYY-mm-dd format

    '''
    def __init__(self, db_root_dir : str):
        self.db_dir = db_root_dir

    def access_log(self, key : str):
        '''Return access log associated with key, if any

        Will throw if the store has no logs asssociated with the key

        @sa item_key

        @param {str} key lookup key
        @return {AccessLog} access log associated with the key, if
                any, None otherwise

        '''
        if not os.path.exists(key):
            return None

        fd = gzip.open(key, 'r')
        return AccessLog.load(fd)

    def item_key(self, row : list):
        '''Return key used for locating a row in a CF access log

        Associated CF logs with the key may be nonexistent

        Implementation uses only the date, so row can be replaced with
        ['YYYY-mm-dd'] format to fetch keys associated with a logs for
        a day

        @param {list} row CF row data
        @return {str} lookup key for access log for a day

        '''

        date_str = row[0]
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        basedir = os.path.join(self.db_dir,
                               str(dt.year),
                               '{:02}'.format(dt.month))
        return os.path.join(basedir, '{}.gz'.format(date_str))

    def overwrite(self, key : str, log : AccessLog):
        '''Overwrite existing data associated with `key

        @param {str} key key used to locate record-set
        @param {AccessLog} log accesslog to overwrite existing content
        @return None

        '''
        dirname = os.path.dirname(key)
        os.makedirs(dirname, exist_ok=True)
        fd = gzip.open(key, 'wb')
        log.dump(fd)

    def list_keys_ranged(self, t0 : str, t1 : str):
        # Fetch all keys
        keys = self.list_keys()
        # Filter keys outside the range
        t0_dt = datetime.strptime(t0, '%Y-%m-%d')
        t1_dt = datetime.strptime(t1, '%Y-%m-%d')
        ret = []
        for k in keys:
            match = re.search('(\d{4}-\d{2}-\d{2}).gz', k)
            dt = datetime.strptime(match.group(1), '%Y-%m-%d')
            if (dt >= t0_dt) and (dt <= t1_dt):
                ret.append(k)
        return sorted(ret)

    def list_keys(self, **kwargs):
        '''Return list of available keys in store

        @sa item_key
        @sa access_log

        @param kwargs {
           date_range = [t0, t1]
        }

        @return {list} Full list of available keys

        '''
        if 'date_range' in kwargs:
            r = kwargs['date_range']
            return self.list_keys_ranged(r[0], r[1])

        p = os.path.join(self.db_dir, '**/*.gz')
        ret = glob.glob(p, recursive=True)
        return sorted(ret)

    def delete(self, key : str):
        ''' Remove records associated with `key`, if any

        @param {str} key key identifying the access log
        @return {bool} True if successful, false otherwise
        '''
        if not os.path.exists(key):
            return False

        try:
            os.remove(key)
        except:
            return False
        return true
