#!/usr/bin/env python

import json
import math

from json import JSONEncoder
from art.splqueryutils.sessions import *

def main():

    inv_queries = {}
    for users in  get_user_sessions(limit=800*BYTES_IN_MB, remove_autorecurring=True):
        invert_user_session_query_grouping(users, inv_queries)
        print "Finished inverting queries."
    queries = inv_queries.values()
    compute_entropy(queries)
    print "Finished computing entropy."
    #put_json_files(queries, 'query_entropy', encoder=QueryEntropyEncoder)
    
    queries.sort(key=lambda x: x.user_entropy, reverse=True)
    put_json_files(queries[:1000], 'high_user_entropy', encoder=QueryEntropyEncoder)
    queries.sort(key=lambda x: x.session_entropy, reverse=True)
    put_json_files(queries[:1000], 'high_session_entropy', encoder=QueryEntropyEncoder)

def compute_and_output_entropy(user_sessions):

    queries = invert_user_session_query_grouping(user_sessions)
    compute_entropy(queries)
    output_entropy(queries)

def invert_user_session_query_grouping(user_sessions, inv_queries):
    
    for user in user_sessions:
        
        for (session_id, session) in user.sessions.iteritems():
            
            session_uid = user.name + '.' +  str(session_id)
            
            for query in session.queries:
                
                q = None
                try:
                    q = inv_queries[query.text]
                except KeyError:
                    inv_queries[query.text] = Query(query.text, 0, None)
                    q = inv_queries[query.text]

                try:
                    q.total_issue_cnt += 1
                except AttributeError:
                    q.total_issue_cnt = 1
            
                try:      
                    q.user_issue_cnt[user.name] +=1 
                except AttributeError:
                    q.user_issue_cnt = {}
                except KeyError:
                    q.user_issue_cnt[user.name] = 1

                try:
                    q.session_issue_cnt[session_uid] += 1
                except AttributeError:
                    q.session_issue_cnt = {}
                except KeyError:
                    q.session_issue_cnt[session_uid] = 1

    
def compute_entropy(queries):

    for query in queries:
        
        total = query.total_issue_cnt
        query.user_entropy = 0.
        query.session_entropy = 0.

        for (user, count) in query.user_issue_cnt.iteritems():
            p = float(count) / float(total)
            query.user_entropy += -.1*p*math.log(p, 2.)
            
        for (session, count) in query.session_issue_cnt.iteritems():
            p = float(count) / float(total)
            query.session_entropy += -.1*p*math.log(p, 2.)

def output_entropy(queries):

    queries.sort(key=lambda x: x.user_entropy)
    lowest_user_entropy_query = queries[0]
    highest_user_entropy_query = queries[len(queries)-1]

    queries.sort(key=lambda x: x.session_entropy)
    lowest_session_entropy_query = queries[0]
    highest_session_entropy_query = queries[len(queries)-1]
    
    print "Highest per-user query entropy: " + str(highest_user_entropy_query.user_entropy) 
    print "\t corresponding query: " + highest_user_entropy_query.text
    print "Lowest per-user query entropy: " + str(lowest_user_entropy_query.user_entropy) 
    print "\t corresponding query: " + lowest_user_entropy_query.text
    print "Highest per-session query entropy: " + str(highest_session_entropy_query.session_entropy)
    print "\t corresponding query: " + highest_session_entropy_query.text
    print "Lowest per-session query entropy: " + str(lowest_session_entropy_query.session_entropy)
    print "\t corresponding query: " + lowest_session_entropy_query.text
    
    out = open('entropy.json', 'w')
    for query in queries:
        json.dump(query, out, sort_keys=True, indent=4, separators=(',', ': '), cls=QueryEntropyEncoder)

    print "See entropy.json for the full list of queries and their entropy."

class QueryEntropyEncoder(JSONEncoder):

    def encode(self, obj):
        query_dict = {}
        query_dict['text'] = obj.text
        query_dict['total_issue_cnt'] = obj.total_issue_cnt
        query_dict['user_entropy'] = obj.user_entropy
        query_dict['session_entropy'] = obj.session_entropy
        return query_dict

    def default(self, obj):
        if isinstance(obj, Query):
            return self.encode(obj)
        return JSONEncoder.default(self, obj)

if __name__ == "__main__":
    main()
