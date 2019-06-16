import os, io, gzip, types, re
import boto3, botocore
from collections import OrderedDict
from io import TextIOWrapper
from gzip import GzipFile
from datetime import datetime



__DATE_COL = 0
__TIME_COL = 1
__REQID_COL = 14


# used -- explicitely tested
def __version(fd):
    ret = ''
    line = next(iter(fd))#.readline()
    split = line.rstrip().split(' ')
    if len(split) <= 1:
        return ''
    return split[1]

# used
def sort_fn(row):
    date_string = '{} {}'.format(row[__DATE_COL], row[__TIME_COL])
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')


# Used -- explicitely tested
def __headers(fd, ver):
    if ver != '1.0':
        return []

    line = next(iter(fd))#.readline()
    fields = line.rstrip().split(' ')
    # ignore the #fields item
    return fields[1:]


# used -- explicitely tested
def __rows(fd, version):
    '''Generator yielding rows from a text-encoded file-like descriptor
    for accesslog

    If the file is gzipped, wrap the descriptor in TextIOWrapper, e.g.
       fd = TextIOWrapper(gzipped_fd, encoding='utf-8')

    @param {file} fd text-encoded file descriptor
    @param {string} version version header
    @return next available row in the access log

    '''
    if version != '1.0':
        return []

    for line in fd:
        yield line.rstrip().split('\t')


# implicitely tested
def parse(fd):
    # Turn into generator if not a generator
    fdi = iter(fd)
    ver = __version(fdi)
    heads = __headers(fdi, ver)

    # Get content
    table = []
    for i, row in enumerate(__rows(fdi, ver)):
        table.append(row)
    return ver, heads, table


def query(col, q):
    def wrap(func):
        def selector(row, headerMap, index):
            if (re.search(q, row[headerMap[col]]) == None):
                return False
            else:
                return func(row, headerMap, index)
        return selector
    return wrap


class AccessLog():
    '''DB-less cloudfront accesslog

    @sa DataStoreLocal
    @sa DataStoreS3

    '''

    def __init__(self, ver, head, rows):
        self.version = ver
        self.headers = head
        self.rows = rows
        self.date_col = 0
        self.time_col = 1
        self.reqid_col = 14

        self.column_map = {}
        for i, h in enumerate(self.headers):
            self.column_map[h] = i

    def cell(self, index : int, column : str):
        column_index = self.column_map[column]
        return self.rows[index][column_index]

    def column(self, column : str):
        column_index = self.column_map[column]
        ret = []
        for row in self.rows:
            ret.append(row[column_index])
        return ret

    @staticmethod
    def load(fd):
        '''Load contents of a accesslog file

        Input function must support getting lines line by line, e.g.
          - gzipped file object,
          - bytes stream object,
          - text file object,
          - array of content

        @param {iterator-liek} file object-like descriptor
        @return {AccessLog} sorted data as a new AcessLog object
        '''

        nfd = fd
        if ((isinstance(fd, gzip.GzipFile)) or (isinstance(fd, io.BytesIO))):
            nfd = TextIOWrapper(fd)
        elif isinstance(fd, botocore.response.StreamingBody):
            gzipped = GzipFile(None, 'rb', fileobj=fd)
            nfd = TextIOWrapper(gzipped)

        ver, head, rows_ = parse(nfd)
        ret = AccessLog(ver, head, rows_)
        return ret.sort()

    def sort(self):
        '''Sort the contents of the access log by date and time

        Does not remove duplicate elements

        @return {AccessLog} Sorted version of self

        '''
        self.rows = sorted(self.rows, key=sort_fn)
        return self


    def concatenate(self, other):
        '''Concatenate other log to existing log

        Both logs must have the same versions, otherwise throws an
        exception.

        Does not sort or remove duplicate entries

        @param {AccessLog} other accesslog to be merged to current lgo
        @return modified version of self
        '''

        if (self.version != other.version):
            msg = 'Mismatch between versions ({}, {}'.format(self.version,
                                                             other.version)

        self.rows += other.rows
        return self

    # def select(self, query):
    #     ret_rows = []
    #     for i,row in enumerate(self.rows):
    #         if query(row, self.column_map, i):
    #             ret_rows.append(row)
    #     return AccessLog(self.version, self.headers, ret_rows)

    def remove_duplicates(self):
        '''

        '''
        od = OrderedDict()
        for row in self.rows:
            # Use reqid, date and time for the key
            key = row[self.reqid_col] \
                + row[self.date_col] \
                + row[self.time_col]
            od[key] = row

        uniques = []
        for k,row in od.items():
            uniques.append(row)

        self.rows = uniques
        return self

    def pop(self, selector):
        '''

        @param {function} selector
        @return {AccessLog} accesslog satisfying selector
        '''
        new_rows = []
        del_list = []
        # Fetch list of items to be removed
        for i,r in enumerate(self.rows):
            select, cont = selector(r)
            if select:
                new_rows.append(r)
                del_list.append(i)
            if not cont:
                break

        # Remove items
        for i in del_list[::-1]:
            del self.rows[i]

        return AccessLog(self.version, self.headers, new_rows)

    def row_datetime(self, row_index):
        date_string = '{} {}'.format(self.rows[row_index][self.date_col],
                                     self.rows[row_index][self.time_col])
        return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

    def record_count(self):
        return len(self.rows)

    def dump(self, fd, sort_data=True):
        '''Dump accesslog file data to gzip file descriptor

        @param {gzip.file} fd gzip file descriptor
        @param {bool} sort_data if true, sorts data before dumping
        @return None
        '''
        if (sort_data):
            self.sort()

        fd.write(bytearray('Version: {}\n'.format(self.version), 'utf-8'))
        fd.write(bytearray('#Fields: {}\n'.format(' '.join(self.headers)),
                           'utf-8'))
        for row in self.rows:
            fd.write(bytearray('{}\n'.format('\t'.join(row)), 'utf-8'))


def group_by_date_generator(access_log : AccessLog):
    '''Generator for outputting records from the same date

    Starts at the top of the log, and yields logs that have the same
    date till end of the log. Does not explicitely sort the data

    @param {AccessLog} access_log input accedsslog
    @return {Generator} new accesslog with records at same date
    '''
    N = access_log.record_count()

    if N < 1:
        raise StopIteration()

    T = access_log.rows[0][__DATE_COL]
    rec_buffer = []
    for i in range(N):
        if (access_log.rows[i][__DATE_COL] == T):
            rec_buffer.append(access_log.rows[i])
        else:
            yield AccessLog(access_log.version,
                            access_log.headers,
                            rec_buffer)
            rec_buffer = [access_log.rows[i]]
            T = access_log.rows[i][__DATE_COL]

    if len(rec_buffer) > 0:
        yield AccessLog(access_log.version,
                        access_log.headers,
                        rec_buffer)


def pop_first_differing_dates(log : AccessLog):
    '''Remove first set of records that have the same date different from
    others

    - Does not sort the records before popping.
    - If all records have the same dates, will return None.

    @param {AccessLog} log input accesslog data
    @return {AccessLog, None} First set of records that have the same
            dates. Returns none if all records have the same dates

    '''
    N = log.record_count()
    if (N == 0):
        return None

    t0 = log.rows[0][0]
    tN = log.rows[-1][0]
    if (tN == t0):
        return None

    selector = lambda row : (row[0] == t0, row[0] == t0)
    return log.pop(selector)