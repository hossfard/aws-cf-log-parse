#!/usr/bin/python3

import os, sys
import unittest

__dirname = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(__dirname, '../src'))
from cf_accesslog import AccessLog
from cf_datastorelocal import DataStoreLocal


class TestDataStoreLocalClass(unittest.TestCase):
    def test_item_key(self):
        store = DataStoreLocal('/tmp/')
        p = store.item_key(['2019-03-01', '12:01:10'])
        self.assertEqual(p, '/tmp/2019/03/2019-03-01.gz')


if __name__ == '__main__':
    unittest.main(verbosity=2)
