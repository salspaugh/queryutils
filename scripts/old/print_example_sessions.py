#!/usr/bin/env python

def print_example_user_sessions(user_sessions):
    for (user, sessions) in user_sessions.iteritems():
        print "User: %s" % user
        print "\tNumber of Sessions: %d" % len(sessions.keys())
        max_session_len = -1
        min_session_len = 1e6
        for (session_id, searches) in sessions.iteritems():
            max_session_len = max(len(searches), max_session_len)
            min_session_len = min(len(searches), min_session_len)
            if len(searches) == 3:
                print "\tSearches:"
                for (time, search) in searches:
                    print "\t\t%s" % search.encode('ascii', 'ignore') 
        print "\tMax Session Length: %d" % max_session_len
        print "\tMin Session Length: %d" % min_session_len
