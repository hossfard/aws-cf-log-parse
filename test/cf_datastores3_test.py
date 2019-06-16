#!/usr/bin/python3

import unittest

import boto3
import os, io, sys, itertools
from unittest.mock import MagicMock, call


__dirname = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(__dirname, '../src'))
from cf_accesslog import AccessLog
from cf_datastores3 import DataStoreS3


class TestDataStoreS3Class(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = boto3.Session()

    def test_ctor(self):
        store = DataStoreS3(bucket='foo', session=self.session)
        self.assertEqual('foo', store.bucket)
        self.assertEqual(store.session, self.session)

    # Test s3.get_object gets called with current parameters
    def test_access_log(self):
        bucket_name = 'foo-bar'
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        content_data = bytearray('Version: 1.0\n', 'utf-8') \
            + bytearray('#Fields: date time\n', 'utf-8') \
            + bytearray('2019-01-01\t15:12:10\n', 'utf-8') \
            + bytearray('2019-02-06\t15:14:10\n', 'utf-8') \
            + bytearray('2019-03-02\t15:13:10\n', 'utf-8')
        content = io.BytesIO(content_data)
        mock_ret_val = {
            'Body': content
        }
        store.s3.get_object = MagicMock(return_value=mock_ret_val)
        store_response = store.access_log('foo')
        store.s3.get_object.assert_called_with(Bucket=bucket_name, Key='foo')

    # Test s3.delete_objects gets called
    def test_deletelist_list(self):
        bucket_name = 'foo-bar'
        keys = ['a', 'b', 'c', 'd']
        delete_key_val = {'Objects': [
            {
                'Key': key
            } for key in keys
        ]}
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        store.s3.delete_objects = MagicMock()
        store.delete_list(keys=keys)
        store.s3.delete_objects.assert_called_with(Bucket=bucket_name,
                                                   Delete=delete_key_val)

    # Test s3.delete_objects gets called sequentially
    def test_deletelist_sub(self):
        bucket_name = 'foo-bar'
        keys = ['a', 'b', 'c', 'd']
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        store.s3.delete_objects = MagicMock()
        store.delete_list(keys=keys, divide_count=2)
        expected_calls = [
            call(Bucket=bucket_name,
                 Delete={'Objects': [{'Key': 'a'}, {'Key': 'b'}]}),
            call(Bucket=bucket_name,
                 Delete={'Objects': [{'Key': 'c'}, {'Key': 'd'}]}),
        ]
        store.s3.delete_objects.assert_has_calls(expected_calls)

    def test_item_key(self):
        row = ['2019-01-02', '13:10:10']
        bucket_name = 'foo'
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        self.assertEqual(store.item_key(row),
                         '2019/01/2019-01-02.gz')

    # TODO
    def test_overwrite(self):
        bucket_name = 'foo-bar'
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        log = AccessLog('1.0', ['date', 'time'], [
            ['2019-01-01', '15:12:10'],
            ['2019-01-01', '15:13:10'],
            ['2019-01-01', '15:14:10']
        ])
        expected_data = bytearray('Version: 1.0\n', 'utf-8') \
            + bytearray('#Fields: date time\n', 'utf-8') \
            + bytearray('2019-01-01\t15:12:10\n', 'utf-8') \
            + bytearray('2019-01-01\t15:13:10\n', 'utf-8') \
            + bytearray('2019-01-01\t15:14:10\n', 'utf-8')
        store.s3.put_object = MagicMock()
        key = store.item_key(['2019-01-01', '15:12:10'])
        store.overwrite(key, log)
        store.s3.put_object.assert_called_with(Bucket=bucket_name,
                                               ACL = 'private',
                                               Body = expected_data,
                                               Key=key)

if __name__ == '__main__':
    unittest.main(verbosity=2)
