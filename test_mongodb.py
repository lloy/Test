# -*- coding:utf-8 -*-
import pymongo
import time
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

URL = "mongodb://TC-IpCount:27017/newlog"
parse = OptionParser()
parse.add_option('-c',
                 '--config',
                 type='int',
                 dest='action',
                 help="to input type for test mongodb, type as follow : \
                         [0]: a single record & insert \
                         [1]: multiple record & insert \
                         [2]: multiple big record & insert \
                         [3]: safe & insert                 \
                         [4]: concurrent safe& insert           \
                         [5]: concurrent record & insert    \
                         [6]: concurrent big record & insert \
                         [7]: a single record & search      \
                         [8]: concurrent & search               \
                         [9]: supply multiple record & search   \
                         [10]: supply multiple record and sort & search \
                         [11]: supply multiple record and skip & search \
                         [12]: all record & search")


def func_time(func):

    def _wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        print 'runtime:', time.time() - start
    return _wrapper


class Test(object):
    def __init__(self, url):
        self.conn = pymongo.MongoClient(url)


class SingleProcessTest(Test):
    def __init__(self, url):
        super(SingleProcessTest, self).__init__(url)
        options = pymongo.uri_parser.parse_uri(url)
        self.db = getattr(self.conn, options['database'], None)

    @func_time
    def single_record_insert(self):
        pass

    @func_time
    def multiple_record_insert(self):
        pass

    @func_time
    def multiple_big_record_insert(self):
        pass

    @func_time
    def safe_insert(self):
        pass

    @func_time
    def single_record_search(self):
        self.db.log.find_one()

    @func_time
    def multiple_record_search(self):
        pass

    @func_time
    def multiple_record_sort(self):
        pass

    @func_time
    def multiple_record_skip(self):
        pass


class MultiProcessTest(Test):
    def __init__(self, url):
        super(MultiProcessTest, self).__init__(url)

    @func_time
    def concurrent_safe(self):
        pass

    @func_time
    def concurrent_record_insert(self):
        pass

    @func_time
    def concurrent_big_record_insert(self):
        pass


def main():
    (options, args) = parse.parse_args()
    if not options.action:
        parse.error('not configure action, please read help')

    if options.action == 0:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 1:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return
    if options.action == 2:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 3:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 4:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 5:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 6:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 7:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 8:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 9:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 10:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 11:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return

    if options.action == 12:
        st = SingleProcessTest(URL)
        st.single_record_search()
        return


if __name__ == '__main__':
    main()
