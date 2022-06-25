#!/usr/bin/python3

from . import cf_accesslog as AL



def archive(keys : list, InDataStore, OutDataStore, delete_from_instore : bool = False):
    '''Fetch accesslog data from bucket, parse, store

    It's highly recommended for object_list to be sorted in the order
    of the internal records. Otherwise, writing can be slower, or, if
    the OutDataStore is S3, it can result in unexpected behavior due
    to its eventual-consistency behavior.

    @param {list} keys list of keys to process
    @param {DataStoreBase} InDataStore input archive data store
    @param {DataStoreBase} OutDataStore output archive data store
    @param {bool} delete_from_instore specifies whether processed
                  files are removed from instore
    @return {list} list of keys that were processed

    '''
    delete_list = []

    # List of s3 keys to fetch
    if len(keys) == 0:
        print('Nothing to do')
        return delete_list

    log = None
    for i, key in enumerate(keys):
        print ('processing {}'.format(key))

        # Fetch the object
        access_log = InDataStore.access_log(key)
        if access_log is None:
            continue

        if log is None:
            log = access_log
        else:
            log = log.concatenate(access_log)

        # Dump the first records belonging to the same date
        dump_log = AL.pop_first_differing_dates(log)
        if dump_log is not None:
            dump_log.sort().remove_duplicates()
            out_key = OutDataStore.item_key(dump_log.rows[0])
            OutDataStore.overwrite(out_key, dump_log)

        # Mark the S3 object for deletion
        delete_list.append(key)

    # Dump remainder data to file
    if log.record_count() > 0:
        out_key = OutDataStore.item_key(log.rows[0])
        existing_rec = OutDataStore.access_log(out_key)
        if existing_rec is not None:
            log.concatenate(existing_rec)
        log.sort().remove_duplicates()
        OutDataStore.overwrite(out_key, log)

    if delete_from_instore:
        InDataStore.delete_list(keys=delete_list)

    return delete_list
