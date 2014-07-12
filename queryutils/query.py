from json import JSONEncoder
from numpy import ceil, floor, histogram, log, mean

EPSILON = 1e-4
ENTROPY_NBUCKETS = 1e4
MAX_INTERVAL = 1e6 # TODO: This shouldn't be hard-coded.
NCHARS = 100
SECONDS = 30.

def wrap_text(text):
    current = text
    wrapped = []
    while len(current) >= NCHARS:
        wrapped.append(current[:NCHARS])
        current = current[NCHARS:]
    wrapped.append(current)
    return "\n\t\t\t\t\t".join(wrapped)

class QueryGroup(object):

    def __init__(self, query):
        self.query = query
        self.copies = []
        self.interarrivals = None

    def copies_this_user(self):
        return [q for q in self.copies if q.user_id == self.query.user_id]

    def number_of_copies(self):
        return len(self.copies)

    def number_of_copies_this_user(self):
        return len(self.copies_this_user())

    def number_of_distinct_users(self):
        u = [q.user_id for q in self.copies]
        return len(set(u))

    def interarrival_intervals(self):
        intervals = []
        relevant_queries = self.copies_this_user()
        relevant_queries.sort(key=lambda x: x.time)
        for idx, query in enumerate(relevant_queries[:-1]):
            curr = query
            next = relevant_queries[idx+1]
            intervals.append(next.time - curr.time)
        self.interarrivals = intervals
        return intervals

    def interarrival_entropy(self):
        if self.interarrivals is None:
            self.interarrivals = self.interarrival_intervals()
        if len(self.interarrivals) <= 1:
            return 0.
        hist, _ = histogram(self.interarrivals, bins=ENTROPY_NBUCKETS, 
            range=(0., MAX_INTERVAL), normed=True)
        # TODO: `normed` is broken, use density, only available in numpy>=?
        return -1*sum([(p)*log((p+EPSILON)) for p in hist])

    def interarrival_consistency(self):
        if self.interarrivals is None:
            self.interarrivals = self.interarrival_intervals()
        if len(self.interarrivals) <= 1:
            return 1.
        avg = mean(self.interarrivals)
        scaled = self.interarrivals / avg
        close = [1. if x < 1.1 and x > .9 else 0. for x in scaled]
        interarrival_consistency = mean(close)
        return interarrival_consistency

    def interarrival_clockness(self):
        if self.interarrivals is None:
            self.interarrivals = self.interarrival_intervals()
        if len(self.interarrivals) == 0:
            return -1. # TODO: What is a reasonable value here?
        rounded = [round(n) for n in self.interarrivals]
        clocked = [min(n%SECONDS, SECONDS - n%SECONDS)/SECONDS 
            for n in self.interarrivals] 
        return 1. - mean(clocked)

    def __str__(self):
        return """
        ID: %s
        Query Text: %s
        This user: %s
        Copies: %d
        Distinct users: %d
        Entropy: %f
        Consistency: %f
        Clockness: %f
        Interarrivals: %s
        """ % (str(self.query.id),
            wrap_text(self.query.text), 
            self.query.user_id, 
            self.number_of_copies(),
            self.number_of_distinct_users(),
            self.interarrival_entropy(),
            self.interarrival_consistency(),
            self.interarrival_clockness(),
            wrap_text(str(self.interarrivals)))

class Query(object):

    def __init__(self, text, time):

        self.text = text
        self.time = float(time)

        self.user = None
        self.is_interactive = False
        self.is_suspicious = False
        self.parsetree = None

        self.execution_time = None
        self.earliest_event = None
        self.latest_event = None
        self.range = None # latest_event - earliest_event
        self.is_realtime = False 

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
