from logging import getLogger as get_logger
from os.path import isfile, isdir
from queryutils.session import Session
from queryutils.parse import parse_query
from queryutils.versions import Version
from queryutils.source import DataSource

NEW_SESSION_THRESH_SECS = 30. * 60.

logger = get_logger("queryutils")

class Files(DataSource):
    """Represents a source storing Splunk queries.
    """

    def __init__(self, path, module, version):
        """Create a File object for accessing Splunk queries.

        This should never be called directly. Instead, call the subclasses
        of this class contained in this module (CSVFiles or JSONFiles).

        :param self: The object being created
        :type self: File
        :param path: The path to the given file object
        :type path: str
        :param module: The module to read the data (jsonparser or csvparser)
        :type module: module
        :param version: The format the Splunk queries are in
        :type version: str (one of the attributes of queryutils.Version)
        :rtype: Files 
        """
        self.path = path
        self.module = module
        self.version = version
        super(Files, self).__init__()

    def connect(self):
        # TODO: Delete me?
        pass

    def close(self):
        # TODO: Delete me?
        pass

    def get_users(self):
        """Return a generator that yields users from the current source.

        :param self: The current object
        :type self: File
        :rtype: generator
        """
        for user in self.get_users_with_queries():
            yield user

    def get_queries(self):
        """Return a generator that yields queries from the current source.
        
        :param self: The current object
        :type self: File
        :rtype: generator
        """
        count = 0
        for user in self.get_users_with_queries():
            for query in user.queries:
                query.query_id = count
                yield query
                count += 1

    def get_parsetrees(self):
        """Return a generator that yields parsetrees from the current source.

        :param self: The current object
        :type self: File
        :rtype: generator
        """
        for query in self.get_queries():
            parsetree = parse_query(query)
            if parsetree:
                parsetree.query_id = query.query_id
                yield parsetree

    def get_sessions(self):
        """Return a generator that yields sessions from the current source.

        :param self: The current object
        :type self: File
        :rtype: generator
        """
        texts = self.get_possibly_suspicious_texts()
        for user in self.get_users():
            self.remove_noninteractive_queries_by_search_type(user, version=self.version)
            user.suspicious = self.remove_suspicious_queries(user, texts)
            self.extract_sessions_from_user(user)
            for (num, session) in user.sessions.iteritems():
                yield session

    def get_possibly_suspicious_texts(self):
        # TODO: Delete me.
        return [] # TODO: Remove this portion of the code -- bad way to detect this.

    def get_users_with_queries(self):
        """Return a generator that yields users from the current source.
        Returns the queries along with the users.

        Called by get_users in this module.

        :param self: The current object
        :type self: File
        :rtype: generator
        """
        get_users = None
        users = {}
        if isfile(self.path):
            get_users = self.module.get_users_from_file
        if isdir(self.path):
            get_users = self.module.get_users_from_directory
        if get_users is None: # TODO: Raise error.
            print "Non-existent path:", self.path
            exit()
        get_users(self.path, users)
        for user in users.values():
            yield user

    def get_users_with_sessions(self):
        """Return a generator that yields users from the current source.
        Returns the sessions and queries along with the users.

        :param self: The current object
        :type self: File
        :rtype: generator
        """
        
        texts = self.get_possibly_suspicious_texts()
        for user in self.get_users():
            self.remove_noninteractive_queries_by_search_type(user, version=self.version)
            user.suspicious = self.remove_suspicious_queries(user, texts)
            self.extract_sessions_from_user(user)
            yield user

    def remove_noninteractive_queries_by_search_type(self, user, version=Version.FORMAT_2014):
        """Label noninteractive queries as such and place them into separate list.
    
        TODO: Move this elsewhere.

        :param self: The current object
        :type self: File
        :param user: The given user whose queries to separate
        :type user: User
        :param version: The version of Splunk query data
        :type version: str (one of queryutils.Version attributes)
        :rtype: generator
        """
        if version == Version.FORMAT_2014:
            handgenerated = "adhoc"
            user.noninteractive_queries = [query for query in user.queries if query.search_type != handgenerated]
            user.interactive_queries = [query for query in user.queries if query.search_type == handgenerated]
        elif version in [Version.FORMAT_2012, Version.FORMAT_2013]:
            handgenerated = "historical"
            user.noninteractive_queries = [query for query in user.queries if query.search_type != handgenerated]
            user.interactive_queries = [query for query in user.queries if query.search_type == handgenerated]
        else:
            print "Unknown data version -- please provide a known version." # TODO: Raise error.
        for query in user.noninteractive_queries:
            query.is_interactive = False
        for query in user.interactive_queries:
            query.is_interactive = True

    def remove_suspicious_queries(self, user, texts):
        # TODO: Delete me.
        return True



class JSONFiles(Files):
    """Represents a source storing Splunk queries in JSON format.
    """

    def __init__(self, path, version):
        """Create a JSONFiles object.

        :param self: The object being created
        :type self: File
        :param path: The path to the given file object
        :type path: str
        :param version: The format the Splunk queries are in
        :type version: str (one of the attributes of queryutils.Version)
        :rtype: JSONFiles 
        """
        import jsonparser
        super(JSONFiles, self).__init__(path, jsonparser, version)


class CSVFiles(Files):
    """Represents a source storing Splunk queries in CSV format.
    """
    
    def __init__(self, path, version):
        """Create a CSVFiles object.

        :param self: The object being created
        :type self: File
        :param path: The path to the given file object
        :type path: str
        :param version: The format the Splunk queries are in
        :type version: str (one of the attributes of queryutils.Version)
        :rtype: CSVFiles 
        """
        import csvparser
        super(CSVFiles, self).__init__(path, csvparser, version)
