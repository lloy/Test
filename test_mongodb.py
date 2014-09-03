# -*- coding:utf-8 -*-
import pymongo
import time
import uuid
from optparse import OptionParser

"""
    对mongo db 进行压力测试，可以选择下列选项进行测试
    1: 单条小数据，动作插入
    2：多条小数据，动作插入
    3：多条大数据，动作插入
    4：安全插入
    5：并发安全插入
    6：并发大数据插入
    7：并发小数据插入
    8：单条数据查询
    9：并发查询
    10：根据查询的记录的多少查询
    11：根据查询的记录的多少并排序查询
    12: 根据查询的记录的多少并切片查询
    13: 查询所有数据"
"""

URL = "mongodb://TC-IpCount:27017/test"
parse = OptionParser()
parse.add_option('-c',
                 '--config',
                 type='int',
                 dest='action',
                 help="to input type for test mongodb, type as follow : \
                         [1]: a single record & insert \
                         [2]: multiple record & insert \
                         [3]: multiple big record & insert \
                         [4]: safe & insert                 \
                         [5]: concurrent safe& insert           \
                         [6]: concurrent record & insert    \
                         [7]: concurrent big record & insert \
                         [8]: a single record & search      \
                         [9]: concurrent & search               \
                         [10]: supply multiple record & search   \
                         [11]: supply multiple record and sort & search \
                         [12]: supply multiple record and skip & search \
                         [13]: all record & search")

SAMPLE = {'1k': './1K',
          '10k': './10K',
          '100k': './100K',
          '1m': './1M'}


def func_time(func):
    def _wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        print 'runtime:', time.time() - start
    return _wrapper


class NotImplementedError(Exception):
    pass


class Test(object):
    def __init__(self, url):
        self.conn = pymongo.MongoClient(url)


class SingleProcessTest(Test):
    def __init__(self, url):
        super(SingleProcessTest, self).__init__(url)
        options = pymongo.uri_parser.parse_uri(url)
        self.db = getattr(self.conn, options['database'], None)

    def _insert(self, text):
        self.db.log.insert({'uuid': uuid.uuid1(), 'text': text})

    @func_time
    def single_record_insert(self, record):
        with open(record, 'r') as f:
            text = str(f.readlines())
        self._insert(text)

    @func_time
    def _readtext(self, text, num):
        for i in range(1, num):
            self._insert(text)

    def multiple_record_insert(self, record, num):
        with open(record, 'r') as f:
            text = str(f.readlines())
        self._readtext(text, num)

    def multiple_big_record_insert(self, record, num):
        with open(record, 'r') as f:
            text = str(f.readlines())
        self._readtext(text, num)

    @func_time
    def safe_insert(self):
        raise NotImplementedError('not implement')

    @func_time
    def single_record_search(self):
        self.db.log.find_one()

    @func_time
    def multiple_record_search(self):
        self.db.log.find().limit(20)

    @func_time
    def multiple_record_sort(self):
        self.db.log.find().sort('uuid')

    @func_time
    def multiple_record_skip(self):
        self.db.log.find().skip(1).limit(10)

    @func_time
    def find_all(self):
        self.db.log.find()


class MultiProcessTest(Test):
    def __init__(self, url):
        super(MultiProcessTest, self).__init__(url)

    @func_time
    def concurrent_safe(self):
        raise NotImplementedError('not implement')

    @func_time
    def concurrent_record_insert(self):
        raise NotImplementedError('not implement')

    @func_time
    def concurrent_big_record_insert(self):
        raise NotImplementedError('not implement')


def main():
    num = 10000
    (options, args) = parse.parse_args()
    if not options.action:
        parse.error('not configure action, please read help')

    print options.action
    print SAMPLE['10k']
    if options.action == 1:
        print 'insert: 10K'
        st = SingleProcessTest(URL)
        st.single_record_insert(SAMPLE['10k'])
        return

    if options.action == 2:
        print 'a time insert:10K\tcount: %dtimes\t insert: %dK' % (num, num*10)
        st = SingleProcessTest(URL)
        st.multiple_record_insert(SAMPLE['10k'], num)
        return

    if options.action == 3:

        print 'a time insert:1m\tcount: %dtimes\t insert :%dM' % (num, num*1)
        st = SingleProcessTest(URL)
        st.multiple_big_record_insert(SAMPLE['1m'], num)
        return

    if options.action == 4:
        st = SingleProcessTest(URL)
        st.safe_insert()
        return

    if options.action == 5:
        raise NotImplementedError('not implement')

    if options.action == 6:
        raise NotImplementedError('not implement')

    if options.action == 7:
        raise NotImplementedError('not implement')

    if options.action == 8:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 9:
        raise NotImplementedError('not implement')

    if options.action == 10:
        st = SingleProcessTest(URL)
        st.multiple_record_search()
        return

    if options.action == 11:
        st = SingleProcessTest(URL)
        st.multiple_record_sort()
        return

    if options.action == 12:
        st = SingleProcessTest(URL)
        st.multiple_record_skip()
        return

    if options.action == 13:
        st = SingleProcessTest(URL)
        st.find_all()
        return


if __name__ == '__main__':
    main()
