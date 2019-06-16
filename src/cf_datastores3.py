import io, os, itertools
import botocore, boto3
import cf_accesslog as AL
from cf_datastore import DataStoreBase
from cf_accesslog import AccessLog
from datetime import datetime



def grouper(iterable, n, fillvalue=None):
    '''Collect data into fixed-length chunks or blocks

    Taken directly from https://docs.python.org/3/library/itertools.html

    Example:
       grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"

    @param {iter} iterable object
    @param {int} n number of subgroups to create
    @param {any} fillvalue values used to pad remaining items if list
                 is not divisible by `n`
    @return {iter} Iterable object containing maximum of `n` values
    '''
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


class DataStoreS3(DataStoreBase):
    '''GZipped local data store, accessible by date in YYYY-mm-dd format

    '''
    def __init__(self, bucket : str, session : boto3.Session = None):
        self.bucket = bucket
        if session is None:
            self.session = boto3.Session()
        else:
            self.session = session
        self.s3 = self.session.client('s3')

    # tested
    def access_log(self, key : str):
        '''Keys must dates in YYYY-MM-DD format

        Throws if no accesslog exists or connection to AWS fails.

        @param {string} key to file, relative to db_dir
        @return {AccessLog} accesslog associatedwith the key

        '''
        resp =  self.s3.get_object(Bucket=self.bucket, Key=key)
        return AL.AccessLog.load(resp['Body'])

    # tested
    def item_key(self, row : list):
        '''Return the key associated with the record (or would be record)

        Single entry list of the format ['YYYY-mm-dd'] can be used to
        record the data

        @param {list} row accesslog row. Only single date is needed
        '''
        date_str = row[0]
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return '{}/{}/{}.gz'.format(str(dt.year),
                                    '{:02}'.format(dt.month),
                                    date_str)

    # tested
    def overwrite(self, key : str, log : AccessLog):
        '''Overwrite existing data associated with `key

        @sa item_key

        @param {str} key key used to locate record-set. must be
                     generated through item_key
        @param {AccessLog} log accesslog to overwrite existing content
        '''
        bytes = io.BytesIO()
        log.dump(bytes)
        self.s3.put_object(Body = bytes.getvalue(),
                           ACL = 'private',
                           Bucket = self.bucket,
                           Key = key)

    # untested
    def delete(self, key : str):
        ''''Delete a single key

        @parma {str} key associated log to delete from the store
        '''
        return self.delete_list([key], 800)

    # tested
    def delete_list(self, **kwarg):
        '''Delete list of keys

        kwargs = {
           keys: list of keys (required)
           divide_count: how many keys to delete in one call (max 1000)
        }

        @param {list} keys list of keys to delete from bucket
        @param {int} divide_count maximum number of delete requests
               sent per call to API. Note: AWS API limits to 1000 max
               deletes
        @return {bool} True if successful, false otherwise
        '''
        keys = kwarg['keys']
        divide_count = kwarg.get('divide_count', 800)
        if isinstance(keys, str):
            keys = [keys]

        dk = [{'Key': k} for k in keys]

        # Maximum AWS-defined allowable deletes per call is 1000
        N = min(divide_count, len(keys), 1000)

        try:
            # Limit number of consecutive requests number of requests
            for sub in grouper(keys, N):
                dk = [{'Key': k} for k in sub]
                self.s3.delete_objects(Bucket=self.bucket, Delete={'Objects': dk})
        except:
            return False

        return True

