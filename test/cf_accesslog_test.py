#!/usr/bin/python3

import io, os, sys
import unittest
from unittest.mock import MagicMock
from unittest.mock import call

__dirname = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(__dirname, '../src'))
import cf_accesslog as AL


class TestAccessLogModule(unittest.TestCase):
    def test_pop_first_differing_dates(self):
        data = [['2019-01-01', '15:12:10'],
                ['2019-01-01', '17:12:10'],
                ['2019-01-01', '18:12:10'],
                ['2019-01-02', '15:12:10'],
                ['2019-01-02', '13:12:10'],
                ['2019-01-05', '12:12:10'],
                ['2019-01-05', '20:12:10']]

        cls = AL.AccessLog('1.0', ['date', 'time'], data)
        pop = AL.pop_first_differing_dates(cls)
        # First pop
        self.assertEqual(pop.rows, [['2019-01-01', '15:12:10'],
                                    ['2019-01-01', '17:12:10'],
                                    ['2019-01-01', '18:12:10']])

        # Second pop
        pop = AL.pop_first_differing_dates(cls)
        self.assertEqual(pop.rows, [['2019-01-02', '15:12:10'],
                                    ['2019-01-02', '13:12:10']])

        # Third pop .. all records are the same (2019-01-05)
        pop = AL.pop_first_differing_dates(cls)
        self.assertEqual(pop, None)



class TestAccessLogClass(unittest.TestCase):
    def test_load(self):
        fd = ['Version: 1.0',
              '#Fields: date time' + ' '*13 + 'reqid',
              '2019-01-05\t15:12:10' + '\t'*11 + '_a',
              '2019-01-02\t15:12:10' + '\t'*11 + '_b',
              '2019-01-01\t15:12:10' + '\t'*11 + '_c']
        expected = [['2019-01-01', '15:12:10'] + ['']*10 + ['_c'],
                    ['2019-01-02', '15:12:10'] + ['']*10 + ['_b'],
                    ['2019-01-05', '15:12:10'] + ['']*10 + ['_a']]
        cls = AL.AccessLog.load(fd)
        self.assertEqual(cls.version, '1.0')
        self.assertEqual(cls.headers, ['date', 'time'] + ['']*12 + ['reqid'])
        self.assertEqual(cls.rows, expected)

    def test_recordcount(self):
        log = AL.AccessLog('1.0', ['date', 'time'], [['2019-01-05\t15:12:10'],
                                                     ['2019-01-02\t15:12:10'],
                                                     ['2019-01-01\t15:12:10']])
        self.assertEqual(log.record_count(), 3)

    def test_sort(self):
        headers = ['date', 'time']
        version = '1.0'
        fd = [['2019-01-05', '15:12:10'],
              ['2019-01-02', '15:12:10'],
              ['2019-01-01', '15:12:10']]
        cls = AL.AccessLog(version, headers, fd)
        sorted_cls = cls.sort()

        expected_rows = [['2019-01-01', '15:12:10'],
                         ['2019-01-02', '15:12:10'],
                         ['2019-01-05', '15:12:10']]

        # should return itself
        self.assertEqual(cls, sorted_cls)
        # sorted rows
        self.assertEqual(cls.rows, expected_rows)

    def test_concatenate(self):
        # Need request-id header data for sorting
        tb = ['']*12
        headers = ['date', 'time',] + tb + ['reqid']
        fd1 = [['2019-01-05', '15:12:10', 'a'],
               ['2019-01-02', '15:12:10', 'b'],
               ['2019-01-01', '15:12:10', 'c']]
        fd2 = [['2019-01-01', '18:12:10', 'd'],
               ['2019-01-02', '18:12:10', 'e'],
               ['2019-01-03', '18:12:10', 'f']]
        log1 = AL.AccessLog('1.0', headers, fd1)
        log2 = AL.AccessLog('1.0', headers, fd2)
        log1.concatenate(log2)

        expected = [
            ['2019-01-05', '15:12:10', 'a'],
            ['2019-01-02', '15:12:10', 'b'],
            ['2019-01-01', '15:12:10', 'c'],
            ['2019-01-01', '18:12:10', 'd'],
            ['2019-01-02', '18:12:10', 'e'],
            ['2019-01-03', '18:12:10', 'f']
        ]
        self.assertEqual(log1.rows, expected)


    # tested
    @unittest.skip('deprecated method')
    def test_mergesort(self):
        # Need request-id header data for sorting
        tb = ['']*12
        headers = ['date', 'time',] + tb + ['reqid']
        fd1 = [['2019-01-05', '15:12:10'] + tb + ['a'],
               ['2019-01-02', '15:12:10'] + tb + ['b'],
               ['2019-01-01', '15:12:10'] + tb + ['c']]
        fd2 = [['2019-01-01', '18:12:10'] + tb + ['d'],
               ['2019-01-02', '18:12:10'] + tb + ['e'],
               ['2019-01-03', '18:12:10'] + tb + ['f']]
        cls1 = AL.AccessLog('1.0', headers, fd1)
        cls2 = AL.AccessLog('1.0', headers, fd2)
        cls1.mergesort(cls2)

        tb = ['']*12
        expected_rows = [
            ['2019-01-01', '15:12:10'] + tb + ['c'],
            ['2019-01-01', '18:12:10'] + tb + ['d'],
            ['2019-01-02', '15:12:10'] + tb + ['b'],
            ['2019-01-02', '18:12:10'] + tb + ['e'],
            ['2019-01-03', '18:12:10'] + tb + ['f'],
            ['2019-01-05', '15:12:10'] + tb + ['a']
        ]
        # should return itself
        self.assertEqual(cls1.version, '1.0')
        self.assertEqual(cls1.rows, expected_rows)

    # Test that the accesslog data can be popped
    def test_pop(self):
        data = [['2019-01-01', '15:12:10'],
                ['2019-01-01', '15:13:10'],
                ['2019-01-01', '15:13:10'],
                ['2019-01-01', '15:13:10'],
                ['2019-01-01', '15:13:10'],
                ['2019-01-02', '15:14:10']]
        log = AL.AccessLog('1.0', ['date', 'time'], data)
        def selector(row):
            c_date = '2019-01-01'
            s = (row[0] == c_date)
            return (s, s)

        popped = log.pop(selector)
        expected_pop = [['2019-01-01', '15:12:10'],
                        ['2019-01-01', '15:13:10'],
                        ['2019-01-01', '15:13:10'],
                        ['2019-01-01', '15:13:10'],
                        ['2019-01-01', '15:13:10']]
        self.assertEqual(popped.rows, expected_pop)
        self.assertEqual(log.rows, [['2019-01-02', '15:14:10']])

    def test_record_count(self):
        data = [['2019-01-01', '15:12:10'],
                ['2019-03-02', '15:13:10'],
                ['2019-04-03', '15:13:10'],
                ['2019-05-04', '15:13:10'],
                ['2019-01-05', '15:13:10'],
                ['2019-02-06', '15:14:10']]
        # Unsorted
        log = AL.AccessLog('1.0', ['date', 'time'], data)
        self.assertEqual(log.record_count(), len(data))

    def test_row_datetime(self):
        data = [['2019-01-01', '15:12:10'],
                ['2019-03-02', '15:13:10'],
                ['2019-04-03', '15:13:10'],
                ['2019-05-04', '15:13:10'],
                ['2019-01-05', '15:13:10'],
                ['2019-02-06', '15:14:10']]
        log = AL.AccessLog('1.0', ['date', 'time'], data)
        dt = log.row_datetime(2)
        self.assertEqual(dt.year, 2019)
        self.assertEqual(dt.month, 4)
        self.assertEqual(dt.day, 3)
        self.assertEqual(dt.hour, 15)
        self.assertEqual(dt.minute, 13)
        self.assertEqual(dt.second, 10)

    def test_dump(self):
        data = [['2019-01-01', '15:12:10'],
                ['2019-03-02', '15:13:10'],
                ['2019-02-06', '15:14:10']]
        log = AL.AccessLog('1.0', ['date', 'time'], data)

        # Expect data to be sorted
        expected = bytearray('Version: 1.0\n', 'utf-8') \
            + bytearray('#Fields: date time\n', 'utf-8') \
            + bytearray('2019-01-01\t15:12:10\n', 'utf-8') \
            + bytearray('2019-02-06\t15:14:10\n', 'utf-8') \
            + bytearray('2019-03-02\t15:13:10\n', 'utf-8')
        fd = io.BytesIO()
        log.dump(fd)
        self.assertEqual(fd.getvalue(), expected)

    def test_remove_duplicates(self):
        tb = ['']*12
        headers = ['date', 'time'] + tb + ['reqid']
        data = [
            ['2019-01-01', '15:12:10'] + tb + ['c'],
            ['2019-01-01', '18:12:10'] + tb + ['duplicate'],
            ['2019-01-02', '15:12:10'] + tb + ['b'],
            ['2019-01-02', '18:12:10'] + tb + ['e'],
            ['2019-01-01', '18:12:10'] + tb + ['duplicate'],
            ['2019-01-03', '18:12:10'] + tb + ['f'],
            ['2019-01-05', '15:12:10'] + tb + ['a']
        ]
        expected_rows = [
            ['2019-01-01', '15:12:10'] + tb + ['c'],
            ['2019-01-01', '18:12:10'] + tb + ['duplicate'],
            ['2019-01-02', '15:12:10'] + tb + ['b'],
            ['2019-01-02', '18:12:10'] + tb + ['e'],
            ['2019-01-03', '18:12:10'] + tb + ['f'],
            ['2019-01-05', '15:12:10'] + tb + ['a']
        ]
        log = AL.AccessLog('1.0', ['date', 'time'], data)
        log.remove_duplicates()
        self.assertEqual(log.rows, expected_rows)

    def test_group_by_date_generator(self):
        headers = ['date', 'time']
        data = [
            ['2019-01-01', '15:12:10'],
            ['2019-01-01', '18:12:10'],
            ['2019-01-02', '15:12:10'],
            ['2019-01-02', '18:12:10'],
            ['2019-01-03', '18:12:10'],
            ['2019-01-05', '15:12:10']
        ]
        log = AL.AccessLog('1.0', ['date', 'time'], data)
        expected_rows = [
            # 2019-01-01
            [
                ['2019-01-01', '15:12:10'],
                ['2019-01-01', '18:12:10']
            ],
            [
                ['2019-01-02', '15:12:10'],
                ['2019-01-02', '18:12:10'],
            ],
            [
                ['2019-01-03', '18:12:10'],
            ],
            [
                ['2019-01-05', '15:12:10']
            ]
        ]
        for i, sub in enumerate(AL.group_by_date_generator(log)):
            self.assertEqual(sub.rows, expected_rows[i])

    def test_select(self):
        data = [['2019-01-01', '15:12:10'],
                ['2019-01-01', '15:13:10'],
                ['2019-01-01', '15:13:10'],
                ['2019-01-01', '15:18:10'],
                ['2019-01-01', '15:20:10'],
                ['2019-01-02', '15:23:10']]
        log = AL.AccessLog('1.0', ['date', 'time'], data)
        ret = log.select('*', {'time': '15:1[0-9]*'})
        self.assertEqual(ret.rows, [['2019-01-01', '15:12:10'],
                                    ['2019-01-01', '15:13:10'],
                                    ['2019-01-01', '15:13:10'],
                                    ['2019-01-01', '15:18:10']])


if __name__ == '__main__':
    unittest.main(verbosity=2)
