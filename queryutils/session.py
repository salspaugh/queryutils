
import editdist

from data import get_users, Version

from collections import defaultdict
from json import JSONEncoder

NEW_SESSION_THRESH_SECS = 30. * 60.
REPEAT_THRESH_SECS = 1.

class Session(object):

    def __init__(self, id, user):
        self.id = int(id)
        self.user = user
        self.queries = []

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

def get_user_sessions(limit=None, filename="", directory="", version=Version.UNDIAG_2014):
    for users in get_users(limit=limit, filename=filename, directory=directory):
        users_with_sessions = extract_sessions(users, version=version)
        yield users_with_sessions

def extract_sessions(users, version=Version.UNDIAG_2014):
    for user in users:
        remove_autorecurring_queries_by_searchtype(user, version=version)
        extract_sessions_from_user(user)
    return users   

def remove_autorecurring_queries_by_searchtype(user, version=Version.UNDIAG_2014):
    if version == Version.UNDIAG_2014:
        handgenerated = "adhoc"
    elif version in [Version.UNDIAG_2012, Version.STORM_2013]:
        handgenerated = "historical"
    else:
        print "Unknown data version -- please provide a known version." # TODO: Raise error.
    user.autorecurring_queries = [query for query in user.queries if query.searchtype != handgenerated or suspicious(query)]
    user.queries = [query for query in user.queries if query.searchtype == handgenerated and not suspicious(query)]

def suspicious(query):
    q = query.text.strip().lower().replace(" ", "")
    return q == "| metadata type=sourcetypes | search totalcount > 0".replace(" ", "") or \
        q == "|history | head 2000 | search event_count>0 or result_count>0 | dedup search | table search".replace(" ", "") or \
        (q[:15] == "typeaheadprefix") or \
        q.find("| metadata type=sourcetypes | search totalcount > 0".replace(" ", "")) > -1 or \
        q == "| inputlookup splunk_servers_cache | sort sort_rank".replace(" ", "")

def remove_autorecurring_queries_by_time(user):
    unique_queries = defaultdict(list)

    for query in user.queries:
        unique_queries[query.text].append(query)
    
    for (query_text, query_list) in unique_queries.iteritems():
        query_list.sort(key=lambda x: x.time)
        prev_time = curr_time = -1
        for query in query_list:
            curr_time = query.time
            if prev_time == -1:
                prev_time = curr_time
            query.repeat_delta = curr_time - prev_time
            prev_time = curr_time
        
        # ignore the first query because the repeat delta will be zero
        repeat_delta_avg = 1.
        query_list[0].repeat_delta = repeat_delta_avg*10. # just to make sure that if there is only one such query, it doesn"t get removed

        if len(query_list) > 1:
            repeat_delta_avg = sum([q.repeat_delta for q in query_list[1:]]) / len(query_list[1:])    
            query_list[0].repeat_delta = repeat_delta_avg
        for query in query_list:
            query.remove = True
            if abs(query.repeat_delta - repeat_delta_avg) > REPEAT_THRESH_SECS:
                query.remove = False
    
    user.all_queries = user.queries
    user.queries = filter(lambda x: not x.remove, user.all_queries)
    user.autorecurring_queries = []
    autorecurring_queries = filter(lambda x: x.remove, user.all_queries)

    unique_autorecurring_queries = defaultdict(list)
    for query in autorecurring_queries:
        unique_autorecurring_queries[query.text].append(query)
    for (qtext, qlist) in unique_autorecurring_queries.iteritems():
        repeat_delta_avg = sum([q.repeat_delta for q in qlist]) / len(qlist)    
        q = Query(qtext, qlist[0].time, user)
        q.repeat_delta = repeat_delta_avg
        user.autorecurring_queries.append(q)

def extract_sessions_from_user(user):
    if len(user.queries) == 0:
        return
    session_id = 0
    user.queries.sort(key=lambda x: x.time)
    prev_time = curr_time = -1.
    session = Session(session_id, user)
    user.sessions[session_id] = session
    for query in user.queries:
        curr_time = query.time
        if prev_time < 0.:
            prev_time = curr_time
        query.delta = curr_time - prev_time
        if query.delta > NEW_SESSION_THRESH_SECS:
            update_session_duration(user.sessions[session_id])
            session_id += 1
            session = Session(session_id, user)
            user.sessions[session_id] = session
        prev_time = curr_time
        query.session = user.sessions[session_id]
        user.sessions[session_id].queries.append(query)
    update_session_duration(user.sessions[session_id])

def update_session_duration(session):
    first_query = session.queries[0]
    last_query = session.queries[-1]
    session.duration = last_query.time - first_query.time

def compute_intrasession_similarity(sessions, metric):
    for (id, session) in sessions.iteritems():
        session['difference'] = []
        prev_search = ''
        for (time, curr_search) in session['searches']:
            if prev_search == '':
                prev_search = curr_search
                continue
            prev_search_unicode = unicode(prev_search) 
            curr_search_unicode = unicode(curr_search) 
            session['difference'].append(metric(prev_search_unicode, curr_search_unicode))

def normalized_edit_distance(a, b):
    return float(editdist.distance(a, b)) / float(max(len(a), len(b)))

def signed_normalized_edit_distance(a, b):
    sign = (len(b) - len(a)) / (len(b) - len(a))
    return sign * float(editdist.distance(a, b)) / float(max(len(a), len(b)))
