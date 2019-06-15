import os, itertools, abc
import botocore, boto3
import cf_accesslog as AL
from cf_accesslog import AccessLog



class DataStoreBase(abc.ABC):
    def __init__(self):
        return

    @abc.abstractmethod
    def access_log(self, key : str):
        '''Return access-log data associated with key

        @param {AccessLog}
        '''
        return

    @abc.abstractmethod
    def item_key(self, row : list):
        '''Return the key used or would-be-used for storage of a accesslog row

        Implement this function to specify how the records are
        stored. Default implementation uses the date as the record key

        @param {list} row Access log record as a list
        @return {str} item key used for storage of the row data

        '''
        return row[0]

    @abc.abstractmethod
    def overwrite(self, key : str, log : AccessLog):
        '''Overwrite records, if any, associated with key

        Default implementation is no-op

        @param TODO
        @return TODO
        '''
        return

    @abc.abstractmethod
    def delete(self, **kwargs):
        '''Delete all associated data with key

        Default implementation is no-op

        kwargs = {
           Key(s):  Required
        }

        @sa item_key
        @param {str} key Key used to locate the accesslog
        @return TODO

        '''
        return

    # nntd
    def grouper_generator(self):
        '''Generator specifying how records are grouped in storage

        Default implementation groups accesslog records by date

        @param TODO
        @return TODO

        '''
        return AL.group_by_date_generator

    # u
    def store(self, access_log : AccessLog):
        '''Write accesslog data to file

        Will merge with existing data, if any

        @param {AccessLog} access_log
        @return None

        '''
        if (access_log.record_count() == 0):
            return

        for log in self.grouper_generator(access_log):
            # Try block in case the log data is incomplete
            try:
                location_key = self.item_key(log.rows[0])
            except:
                continue

            # If encountered error while trying to open existing log,
            # e.g. no file, bad content, etc, ignore existing data
            try:
                existing_log = self.access_log(location_key)
                # TODO: better to rename mergesort to append and follow it with sort -> remove_dup
                log.concatenate(existing_log)
            except:
                pass

            log.sort().remove_duplicates()
            # get new key -- should be as before if logs weren't
            # manually modified
            location_key = self.item_key(log.rows[0])
            self.overwrite(location_key, log)
        return
