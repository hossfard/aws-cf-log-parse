#!/usr/bin/python3

import os, sys, argparse, boto3


__dirname = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(__dirname, '../src'))
import cf_datastores3 as DS3
import cf_archiver as archiver
from cf_datastores3 import DataStoreS3


def s3_to_s3(inbucket : str, outbucket : str,
             in_prefix : str = '', out_prefix : str = '',
             delete_source : bool = False,
             profile : str = None):
    '''Fetch and archive CF log data from S3 to local drive

    Deletes associates files on S3.

    @param {str} inbucket Input AWS S3 bucket name
    @param {str} outbucket Output AWS S3 bucket name
    @param {str} in_prefix Input AWS S3 bucket prefix
    @param {str} out_prefix Output AWS S3 bucket prefix
    @param {str} delete_source delete files from bucket after processing
    @param {str} profile_name AWS named profile name to use (~/.aws/credentials)
    '''

    session = boto3.Session(profile_name=profile)

    # Get list of unprocessed cloudfront accesslogs from bucket
    # These keys are different than the ones DataStoreS3 creates, but
    # they are interchangable for following use-case
    keys = DS3.list_cf_logkeys(session.client('s3'), inbucket, in_prefix)

    if len(keys) == 0:
        print('Nothing to do')
        return

    # S3 access log data store
    in_store = DataStoreS3(inbucket, session, in_prefix)
    # Location on local drive to store the data
    out_store = DataStoreS3(outbucket, session, out_prefix)

    archiver.archive(keys, in_store, out_store, delete_source)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Archive CF logs to local drive')

    # generate better help text
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('--inbucket', '-i', type=str, required=True,
                          help='S3 source bucket name')
    required.add_argument('--outbucket', '-o', type=str, required=True,
                          help='S3 destination bucket name')

    optional.add_argument('--inbucket-prefix', type=str, default='',
                          help='S3 source bucket prefix')
    optional.add_argument('--outbucket-prefix', type=str, default='',
                          help='S3 destination bucket prefix')
    optional.add_argument('--profile', type=str, default=None,
                          help='AWS profile name under ~/.aws/credentials')
    optional.add_argument('--delete-source', default=False,
                          action='store_true',
                          help='Delete files from S3 once processed')

    args = parser.parse_args()
    s3_to_s3(inbucket=args.inbucket, outbucket=args.outbucket,
             in_prefix=args.inbucket_prefix, out_prefix=args.outbucket_prefix,
             delete_source=args.delete_source, profile=args.profile)
