#!/usr/bin/python3

import os, io, sys, gzip
import boto3
import unittest
from unittest.mock import MagicMock, call

from awslogparse.cf_accesslog import AccessLog
from awslogparse.cf_datastores3 import DataStoreS3
from awslogparse import cf_datastores3 as DS3



class TestDateStoreS3fn(unittest.TestCase):
    def test_is_valid_cf_logkey(self):
        test_data = [
            {
                'test': 'A3HR21C7CND2BQ.2019-06-09-17.ab3a8cd4.gz',
                'expected': True
            },
            {
                'test': 'B3HR21CSCND2BQ.2019-06-09-12.aa3a8ad4.gz',
                'expected': True
            },
            {
                'test': '2019-06-09-12.aa3a8ad4.gz',
                'expected': False
            },
            {
                'test': '2019-06-09.gz',
                'expected': False
            },
        ]
        for i, data in enumerate(test_data):
            with self.subTest('Subtest {}'.format(i)):
                result = DS3.is_valid_cf_logkey(data['test'])
                expected = data['expected']
                self.assertEqual(result, expected)


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

    # Test datastores3.delete gets delegated to delete_objects
    def test_delete(self):
        bucket_name = 'foo-bar'
        keys = ['a']
        delete_key_val = {'Objects': [
            {
                'Key': key
            } for key in keys
        ]}
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        store.delete_list = MagicMock()
        store.delete(keys[0])
        store.delete_list.assert_called_with(keys=keys)

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

    def test_item_key_withprefix(self):
        row = ['2019-01-02', '13:10:10']
        bucket_name = 'foo'
        store = DataStoreS3(bucket=bucket_name, session=self.session, prefix='foo/')
        self.assertEqual(store.item_key(row),
                         'foo/2019/01/2019-01-02.gz')

    # Test s3.put_object gets invoked with right arguments
    def test_overwrite(self):
        bucket_name = 'foo-bar'
        store = DataStoreS3(bucket=bucket_name, session=self.session)
        log = AccessLog('1.0', ['date', 'time'], [
            ['2019-01-01', '15:12:10'],
            ['2019-01-01', '15:13:10'],
            ['2019-01-01', '15:14:10']
        ])
        expected_data = io.BytesIO()
        fo = gzip.open(expected_data, 'wb')
        fo.write(bytearray('Version: 1.0\n', 'utf-8') \
                 + bytearray('#Fields: date time\n', 'utf-8') \
                 + bytearray('2019-01-01\t15:12:10\n', 'utf-8') \
                 + bytearray('2019-01-01\t15:13:10\n', 'utf-8') \
                 + bytearray('2019-01-01\t15:14:10\n', 'utf-8'))
        fo.close()
        store.s3.put_object = MagicMock()
        key = store.item_key(['2019-01-01', '15:12:10'])
        store.overwrite(key, log)
        store.s3.put_object.assert_called_with(Bucket=bucket_name,
                                               ACL = 'private',
                                               Body = expected_data.getvalue(),
                                               Key=key)

    # Test s3.list_objects_v2 is called with right arguments
    def test_list_keys(self):
        bucket_name = 'foo-bar'
        prefix = 'pre'
        store = DataStoreS3(bucket=bucket_name, session=self.session, prefix=prefix)
        store.s3.list_objects_v2 = MagicMock()
        keys = store.list_keys()
        store.s3.list_objects_v2.assert_called_with(Bucket=bucket_name, Prefix=prefix)


    # Test list_keys_ranged gets called with right arguments
    def test_list_keys_ranged(self):
        bucket_name = 'foo-bar'
        prefix = 'pre'
        store = DataStoreS3(bucket=bucket_name, session=self.session, prefix=prefix)
        store.s3.list_objects_v2 = MagicMock()
        store.list_keys_ranged = MagicMock()
        keys = store.list_keys(date_range=['2019-01-01', '2019-01-03'])
        store.list_keys_ranged.assert_called_with('2019-01-01', '2019-01-03')


if __name__ == '__main__':
    unittest.main(verbosity=2)
