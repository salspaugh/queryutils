#!/usr/bin/env python

import json
import math

from json import JSONEncoder
from art.splqueryutils.sessions import *

def main():
   
    gen_sessions = []
    spec_sessions = []
    rep_sessions = []
    for users in  get_user_sessions(limit=800*BYTES_IN_MB, remove_autorecurring=True):
        sessions = compute_session_trends(users) 
        print "Done computing session trends."
        gen_sessions += filter(lambda x: x.generalization, sessions)
        spec_sessions += filter(lambda x: x.specialization, sessions)
        rep_sessions += filter(lambda x: x.repeated_query, sessions)
    
    put_json_files(gen_sessions, 'generalizations', encoder=SessionTrendEncoder)
    put_json_files(spec_sessions, 'specializatons', encoder=SessionTrendEncoder)
    put_json_files(rep_sessions, 'repeated_queries', encoder=SessionTrendEncoder)
    
    num_gen_sessions = len(gen_sessions)
    num_spec_sessions = len(spec_sessions)
    num_rep_sessions = len(rep_sessions)
    print "Number of generalizing sessions: " + str(num_gen_sessions)
    print "Number of specializing sessions: " + str(num_spec_sessions)
    print "Number of repeated query sessions: " + str(num_rep_sessions)
    
    #users = get_user_sessions()
    #compute_and_output_session_trends(users)

def compute_and_output_session_trends(user_sessions):
    sessions = compute_session_trends(user_sessions)
    output_session_trends(sessions)

def compute_session_trends(user_sessions):

    sessions = []

    for user in user_sessions:
        
        for (session_id, session) in user.sessions.iteritems():
            
            session.generalization = True
            session.specialization = True
            session.repeated_query = True
            prev_query = curr_query = -1
           
            if len(session.queries) <= 1: continue

            for query in session.queries:
                
                curr_query = query.text
                
                if prev_query == -1:
                    prev_query = curr_query
                if not prev_query == curr_query:
                    session.repeated_query = False
                if not prev_query.find(curr_query) > -1:
                    session.generalization = False
                if not curr_query.find(prev_query) > -1:
                    session.specialization = False
                prev_query = curr_query
           
            if session.repeated_query:
                session.generalization = False
                session.specialization = False
                
            if session.generalization or session.specialization or session.repeated_query:
                sessions.append(session)
    
    return sessions


def output_session_trends(sessions):

    gen_sessions = filter(lambda x: x.generalization, sessions)
    num_gen_sessions = len(gen_sessions)
    
    spec_sessions = filter(lambda x: x.specialization, sessions)
    num_spec_sessions = len(spec_sessions)
    
    print "Number of generalizing sessions: " + str(num_gen_sessions)
    print "Number of specifying sessions: " + str(num_spec_sessions)

    out = open('session_trends.json', 'w')
    for session in sessions:
        if session.generalization or session.specialization:
            json.dump(session, out, sort_keys=True, indent=4, separators=(',', ': '), cls=SessionTrendEncoder)

class SessionTrendEncoder(JSONEncoder):

    def encode(self, obj):
        session_dict = {}
        session_dict['id'] = obj.id
        session_dict['user'] = obj.user.name
        session_dict['generalization'] = obj.generalization
        session_dict['specialization'] = obj.specialization
        session_dict['repeated_query'] = obj.repeated_query
        query_list = []
        for query in obj.queries:
            query_list.append(query.text)
        session_dict['queries'] = query_list
        return session_dict

    def default(self, obj):
        if isinstance(obj, Session):
            return self.encode(obj)
        return JSONEncoder.default(self, obj)

if __name__ == "__main__":
    main()
