import unittest
from os import path
from queryutils.datasource import Version, CSVFiles, JSONFiles, PostgresDB, SQLite3DB
from queryutils.user import User
from queryutils.session import Session
from queryutils.query import Query
from splparser.parsetree import ParseTreeNode


class DataSourceTestCase(object):

    def is_valid_user(self, obj):
        return isinstance(obj, User) and \
            (isinstance(obj.name, str) or isinstance(obj.name, unicode))

    def is_valid_session(self, obj):
        return isinstance(obj, Session)
    
    def is_valid_query(self, obj):
        return isinstance(obj, Query) and \
            (isinstance(obj.text, str) or isinstance(obj.text, unicode)) and \
            len(obj.text) > 0

    def is_valid_parsetree(self, obj):
        return isinstance(obj, ParseTreeNode) 
    
    def test_init(self):
        source = self.source(*self.args)
        assert source
    
    def test_get_users(self):
        source = self.source(*self.args)
        users = [user for user in source.get_users()]
        assert len(users) == 1 and all([self.is_valid_user(u) for u in users])

    def test_get_sessions(self):
        source = self.source(*self.args)
        sessions = [session for session in source.get_sessions()]
        assert len(sessions) > 0 and all([self.is_valid_session(s) for s in sessions])

    def test_get_queries(self):
        source = self.source(*self.args)
        queries = [query for query in source.get_queries()]
        assert len(queries) == 10 and all([self.is_valid_query(q) for q in queries])

    def test_get_parsetrees(self):
        source = self.source(*self.args)
        parsetrees = [parsetree for parsetree in source.get_parsetrees()]
        assert len(parsetrees) == 10 and all([self.is_valid_parsetree(p) for p in parsetrees])

    def test_get_unique_stages(self):
        source = self.source(*self.args)
        stages = [stage for stage in source.get_unique_stages([])]
        assert all([self.is_valid_parsetree(s) for s in stages])

    def test_get_unique_filters(self):
        source = self.source(*self.args)
        filters = [stage for stage in source.get_unique_filters()]
        assert all([self.is_valid_parsetree(s) for s in filters])

    def test_get_unique_aggregates(self):
        source = self.source(*self.args)
        aggregates = [stage for stage in source.get_unique_aggregates()]
        assert all([self.is_valid_parsetree(s) for s in aggregates])

    def test_get_unique_augments(self):
        source = self.source(*self.args)
        augments = [stage for stage in source.get_unique_augments()]
        assert all([self.is_valid_parsetree(s) for s in augments])


class CSVFilesTestCase(DataSourceTestCase, unittest.TestCase):
    """
    Tests for queryutils.datasource.CSVFiles
    """

    def setUp(self):
        thisdir = path.dirname(path.realpath(__file__))
        self.path = path.join(thisdir, "data/undiag2014.csv")
        self.version = Version.UNDIAG_2014
        self.args = [self.path, self.version]
        self.source = CSVFiles


class JSONFilesTestCase(DataSourceTestCase, unittest.TestCase):
    """
    Tests for queryutils.datasource.JSONFiles
    """

    def setUp(self):
        thisdir = path.dirname(path.realpath(__file__))
        self.path = path.join(thisdir, "data/undiag2012.json")
        self.version = Version.UNDIAG_2012
        self.args = [self.path, self.version]
        self.source = JSONFiles


class PostgresDBTestCase(DataSourceTestCase, unittest.TestCase):
    """
    Tests for queryutils.datasource.PostgresDB
    """

    def setUp(self):
        self.database = "splqueryutils_test_postgres"
        self.user = "splqueryutils_test_postgres"
        self.password = "splqueryutils_test_postgres"
        self.args = [self.database, self.user, self.password]
        self.source = PostgresDB


class SQLite3DBTestCase(DataSourceTestCase, unittest.TestCase):
    """
    Tests for queryutils.datasource.SQLite3DB
    """

    def setUp(self):
        thisdir = path.dirname(path.realpath(__file__))
        self.path = path.join(thisdir, "data/sqlite3.db")
        self.args = [self.path]
        self.source = SQLite3DB


if __name__ == "__main__":
    unittest.main()
