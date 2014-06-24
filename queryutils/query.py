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

class Text(object):

    def __init__(self, text, uid):
        self.text = text
        self.selected_user = uid
        self.all_occurrences = []
        self.all_users = []
        self.selected_occurrences = []
        self.interarrivals = []
        self.id = 0

    def number_of_all_occurrences(self):
        return len(self.all_occurrences)
    
    def number_of_selected_occurrences(self):
        return len(self.selected_occurrences)

    def distinct_users(self):
        return len(set(self.all_users))

    def get_interarrivals(self):
        interarrivals = []
        self.selected_occurrences.sort(key=lambda x: x.time)
        for idx, query in enumerate(self.selected_occurrences[:-1]):
            curr = query
            next = self.selected_occurrences[idx+1]
            interval = next.time - curr.time
            interarrivals.append(interval)
        return interarrivals

    def interarrival_entropy(self):
        if len(self.interarrivals) == 0:
            self.interarrivals = self.get_interarrivals()
        if len(self.interarrivals) < 2:
            return 0.
        print self.interarrivals
        hist, _ = histogram(self.interarrivals, bins=ENTROPY_NBUCKETS, range=(0., MAX_INTERVAL), normed=True)
        return -1*sum([(p)*log((p+EPSILON)) for p in hist])


    def periodicity(self):
        avg = mean(self.interarrivals)
        scaled = self.interarrivals / avg
        close = [1. if x < 1.1 and x > .9 else 0. for x in scaled]
        periodicity = mean(close)
        return periodicity

    def clockness(self):
        rounded = [round(n) for n in self.interarrivals]
        clocked = [min(n%SECONDS, SECONDS - n%SECONDS)/SECONDS for n in self.interarrivals] 
        return mean(clocked)

    def __str__(self):
        return """
        Text: %s
        This user: %s
        All occurrences: %d
        Distinct users: %d
        Entropy: %f
        Periodicity: %f
        Clockness: %f
        Interarrivals: %s
        """ % (wrap_text(self.text), 
            self.selected_user, 
            self.number_of_all_occurrences(),
            self.distinct_users(),
            self.interarrival_entropy(),
            self.periodicity(),
            self.clockness(),
            wrap_text(str(self.interarrivals)))

class Query(object):

    def __init__(self, text, time):

        self.text = text
        self.time = float(time)

        self.user = None
        self.is_interactive = None
        self.is_suspicious = False
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
