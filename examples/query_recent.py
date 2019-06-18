#!/usr/bin/python3
#
# Display data from the past day from archive data
#
# Usage
#  <script-name> --dbpath <path-to-archive-directory
#

import os, sys, argparse
from datetime import datetime, timedelta

__dirname = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(__dirname, '../src'))
from cf_datastorelocal import DataStoreLocal



def query_recent(db_path):
    t0 = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
    t1 = datetime.strftime(datetime.utcnow(), '%Y-%m-%d')

    store = DataStoreLocal(db_path)
    # See
    # https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#LogFileFormat
    # for header description
    res = store.select(['date', 'time', 'c-ip', 'cs-uri-stem', 'cs(User-Agent)']) \
               .where({'sc-status': '200'}) \
               .daterange([t0, t1]) \
               .execute()
    res.display()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Display select data from past date')
    parser.add_argument('--dbpath', type=str, required=True,
                        help='Location on local drive where archive data is stored')

    args = parser.parse_args()
    db_path = os.path.join(os.getcwd(), args.dbpath)

    query_recent(db_path)
