from json import JSONEncoder

class User(object):
    """Represents a user object.
    """

    def __init__(self, name):
        """Create a User object.

        :param self: The object being created
        :type self: User
        :param name: The name of the user
        :type name: str
        :rtype: User
        """
        self.name = name
        self.sessions = {}
        self.queries = []
        self.noninteractive_queries = []
        self.interactive_queries = []
        self.case_id = None
        self.user_type = None


class VerboseUserEncoder(JSONEncoder):


    def encode(self, obj):
        user_dict = {}
        user_dict['name'] = obj.name
        session_dict = {}
        for (session_id, session) in obj.sessions.iteritems():
            session_dict[session_id] = SessionEncoder().default(session)
        query_list = []
        for query in obj.queries:
            query_list.append(QueryEncoder().default(query))
        user_dict['queries'] = query_list
        return user_dict


    def default(self, obj):
        if isinstance(obj, User):
            return self.encode(obj)
        return JSONEncoder.default(self, obj)



class UserEncoder(JSONEncoder):


    def encode(self, obj):
        user_dict = {}
        user_dict['name'] = obj.name
        session_dict = {}
        for (session_id, session) in obj.sessions.iteritems():
            session_dict['id'] = session_id
            query_list = []
            for query in session.queries:
                query_dict = {}
                query_dict['delta'] = query.delta
                query_dict['time'] = query.time
                query_dict['text'] = query.text
                query_list.append(query_dict)
            session_dict['queries'] = query_list
            session_dict['user'] = obj.name
        try:
            autorecurring_query_list = []
            for query in obj.autorecurring_queries:
                query_dict = {}
                query_dict['repeat_delta'] = query.repeat_delta
                query_dict['time'] = query.time
                query_dict['text'] = query.text
                autorecurring_query_list.append(query_dict)
            user_dict['autorecurring_queries'] = autorecurring_query_list
        except AttributeError: 
            print "Not encoding autorecurring queries. No such attribute."
        user_dict['sessions'] = session_dict
        return user_dict


    def default(self, obj):
        if isinstance(obj, User):
            return self.encode(obj)
        return JSONEncoder.default(self, obj)
