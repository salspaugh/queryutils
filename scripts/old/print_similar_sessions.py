#!/usr/bin/env python

from art.splqueryutils.sessions import *

def output_highly_similar_sessions(threshhold=.5):
    out = open('similar_sessions.out', 'w')
    jsonfiles = get_json_files(limit=1000*BYTES_IN_MB)
    all_sessions = sessionize_searches(jsonfiles)
    for (user, user_sessions) in all_sessions.iteritems():
        compute_intrasession_similarity(user_sessions, normalized_edit_distance)
        for (id, session_info) in user_sessions.iteritems():
            if len(session_info['searches']) < 2:
                continue
            average_difference = sum(session_info['difference']) / float(len(session_info['difference']))
            if average_difference < threshhold:
                 out.write('= = = = =\n')
                 out.write(user  + '\t' + 'session ' + str(id) + '\n')
                 for (time, search) in session_info['searches']:
                     out.write('\t' + str(time) + '\t' + search.encode('ascii', 'ignore') + '\n')
                     out.flush()
    out.close()

output_highly_similar_sessions()
