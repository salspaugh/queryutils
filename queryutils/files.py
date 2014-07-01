from logging import getLogger as get_logger
from os.path import isfile, isdir
from queryutils.session import Session
from queryutils.parse import parse_query
from queryutils.versions import Version

NEW_SESSION_THRESH_SECS = 30. * 60.

logger = get_logger("queryutils")

class Files(DataSource):

    def __init__(self, path, module, version):
        self.path = path
        self.module = module
        self.version = version
        super(Files, self).__init__()

    def get_users(self):
        for user in self.get_users_with_queries():
            yield user

    def get_queries(self):
        count = 0
        for user in self.get_users_with_queries():
            for query in user.queries:
                query.query_id = count
                yield query
                count += 1

    def get_parsetrees(self):
        for query in self.get_queries():
            parsetree = parse_query(query)
            if parsetree:
                parsetree.query_id = query.query_id
                yield parsetree

    def get_sessions(self):
        texts = self.get_possibly_suspicious_texts()
        for user in self.get_users():
            self.remove_noninteractive_queries_by_search_type(user, version=self.version)
            user.suspicious = self.remove_suspicious_queries(user, texts)
            self.extract_sessions_from_user(user)
            for (num, session) in user.sessions.iteritems():
                yield session

    def get_users_with_queries(self):
        get_users = None
        if isfile(self.path):
            get_users = self.module.get_users_from_file
        if isdir(self.path):
            get_users = self.module.get_users_from_directory
        if get_users is None: # TODO: Raise error.
            print "Non-existent path:", self.path
            exit()
        for user in get_users(self.path):
            yield user

    def get_users_with_sessions(self):
        texts = self.get_possibly_suspicious_texts()
        for user in self.get_users():
            self.remove_noninteractive_queries_by_search_type(user, version=self.version)
            user.suspicious = self.remove_suspicious_queries(user, texts)
            self.extract_sessions_from_user(user)
            yield user

    def remove_noninteractive_queries_by_search_type(self, user, version=Version.DIAG_2014):
        if version == Version.DIAG_2014:
            handgenerated = "adhoc"
            user.noninteractive_queries = [query for query in user.queries if query.search_type != handgenerated]
            user.interactive_queries = [query for query in user.queries if query.search_type == handgenerated]
        elif version in [Version.DIAG_2012, Version.STORM_2013]:
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
        return True

    def extract_sessions_from_user(self, user):
        if len(user.interactive_queries) == 0:
            return
        session_id = 0
        user.interactive_queries.sort(key=lambda x: x.time)
        prev_time = curr_time = -1.
        session = Session(session_id, user)
        user.sessions[session_id] = session
        for query in user.interactive_queries:
            curr_time = query.time
            if prev_time < 0.:
                prev_time = curr_time
            query.delta = curr_time - prev_time
            if query.delta > NEW_SESSION_THRESH_SECS:
                self.update_session_duration(user.sessions[session_id])
                session_id += 1
                session = Session(session_id, user)
                user.sessions[session_id] = session
            prev_time = curr_time
            query.session = user.sessions[session_id]
            user.sessions[session_id].queries.append(query)
        self.update_session_duration(user.sessions[session_id])

    def update_session_duration(self, session):
        first_query = session.queries[0]
        last_query = session.queries[-1]
        session.duration = last_query.time - first_query.time


class JSONFiles(Files):
    
    def __init__(self, path, version):
        import jsonparser
        super(JSONFiles, self).__init__(path, jsonparser, version)


class CSVFiles(Files):
    
    def __init__(self, path, version):
        import csvparser
        super(CSVFiles, self).__init__(path, csvparser, version)
