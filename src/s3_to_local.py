#!/usr/bin/python3

import os, sys, argparse, boto3
import cf_datastores3 as DS3
import cf_archiver as archiver
from cf_datastorelocal import DataStoreLocal
from cf_datastores3 import DataStoreS3


# TODO: use prefix and specify local db path
def s3_to_local(bucket : str, db_path : str = '',
                delete_source : bool = False,
                bucket_prefix : str = '', profile : str = None):
    '''Fetch and archive CF log data from S3 to local drive

    Deletes associates files on S3.

    @param {str} bucket AWS S3 bucket name
    @param {str} bucket_prefix S3 content prefix
    @param {str} delete_source delete files from bucket after processing
    @param {str} profile_name AWS named profile name to use (~/.aws/credentials)
    '''

    # Path of current script file
    __dirname = os.path.dirname(os.path.realpath(__file__))

    session = boto3.Session(profile_name=profile)

    # Get list of unprocessed cloudfront accesslogs from bucket
    # These keys are different than the ones DataStoreS3 creates, but
    # they are interchangable for following use-case
    keys = DS3.list_cf_logkeys(session.client('s3'), bucket, bucket_prefix)

    if len(keys) == 0:
        print('Nothing to do')
        return

    # S3 access log data store
    in_store = DataStoreS3(bucket, session)
    # Location on local drive to store the data -- TODO
    out_store = DataStoreLocal(os.path.join(__dirname, '../db'))

    archiver.archive(keys, in_store, out_store, delete_source)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Archive CF logs to local drive')

    parser.add_argument('--bucket', type=str, required=True,
                        help='S3 source bucket name')
    parser.add_argument('--dbpath', type=str, required=True,
                        help='Location on local drive to archive')
    parser.add_argument('--bucket-prefix', type=str, default='',
                        help='Bucket content prefix')
    parser.add_argument('--delete-source', type=bool, default=False,
                        choices=(True, False),
                        help='Delete files from s3 once processed')
    parser.add_argument('--profile', type=str, default=None,
                        help='AWS profile name under ~/.aws/credentials')

    args = parser.parse_args()
    db_path = os.path.join(os.getcwd(), args.dbpath)

    s3_to_local(args.bucket, db_path,
                args.delete_source, args.bucket_prefix,
                args.profile)
