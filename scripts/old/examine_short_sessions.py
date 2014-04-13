#!/usr/bin/env python

from art.splqueryutils.sessions import get_user_sessions, SessionEncoder
from art.splqueryutils.jsondata import put_json_files, BYTES_IN_MB

import matplotlib.pyplot as plt
import numpy as np
import pylab

NBINS = 1000.

def main():
    short_sessions, zeros  = get_short_sessions(keepzeros=False)
    print "Number of short sessions: " + str(len(short_sessions))
    print "Number of single-query sessions: " + str(zeros)
    put_json_files(short_sessions, "short_sessions", encoder=SessionEncoder) 

def get_short_sessions(keepzeros=True):
    short_sessions = []
    zeros = 0.
    for users in  get_user_sessions(limit=8*BYTES_IN_MB, remove_autorecurring=True):
        for user in users:
            for (sid, session) in user.sessions.iteritems():
                if session.duration <= 1.:
                    if session.duration == 0.:
                        zeros += 1
                        if keepzeros:
                            short_sessions.append(session)
                    else:
                        short_sessions.append(session)
    short_sessions.sort(key=lambda x: x.duration)
    return short_sessions, zeros

if __name__ == "__main__":
    main()
