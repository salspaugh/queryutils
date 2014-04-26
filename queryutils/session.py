from json import JSONEncoder

class Session(object):

    def __init__(self, id, user):
        self.id = int(id)
        self.user = user
        self.queries = []
        self.session_type = None

class SessionEncoder(JSONEncoder):
   
    def encode(self, obj):
        session_dict = {}
        session_dict['id'] = obj.id
        session_dict['user'] = obj.user.name  
        query_list = []
        for query in obj.queries:
            query_list.append(QueryEncoder().default(query))
        session_dict['queries'] = query_list
        return session_dict

    def default(self, obj):
        if isinstance(obj, Session):
            return self.encode(obj)
        return JSONEncoder.default(self, obj)
