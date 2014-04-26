from json import JSONEncoder

class Query(object):

    def __init__(self, text, time):

        self.text = text
        self.time = float(time)

        self.user = None
        self.is_interactive = None
        self.parsetree = None

        self.execution_time = None
        self.earliest_event = None
        self.latest_event = None
        self.range = None # latest_event - earliest_event
        self.is_realtime = None 

        self.search_type = None
        self.splunk_search_id = None
        self.saved_search_name = None
        
        self.session = None
        
    
    def __repr__(self):
        return "".join([str(self.time), ": ", self.text, "\n"])



class QueryEncoder(JSONEncoder):


    def encode(self, obj):
        query_dict = obj.__dict__
        query_dict['user'] = obj.user.name
        query_dict['is_interactive'] = obj.is_interactive
        if not obj.session is None:
            query_dict['session'] = obj.session.id
        return query_dict


    def default(self, obj):
        if isinstance(obj, Query):
            return self.encode(obj)
        return JSONEncoder.default(self, obj)
