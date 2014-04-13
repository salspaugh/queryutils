#!/usr/bin/env python

from art.splqueryutils.sessions import get_user_sessions, UserEncoder
from art.splqueryutils.jsondata import put_json_files, BYTES_IN_MB

import matplotlib.pyplot as plt
import numpy as np
import pylab

NBINS = 1000.

def main():
    session_lengths, zeros  = compute_session_lengths()
    print "Done computing session lengths."
    print "Number of zero-lengthed sessions: " + str(zeros)
    write_session_lengths(session_lengths)
    plot_session_length_histogram(session_lengths)

def compute_session_lengths(keepzeros=True):
    session_lengths = []
    zeros = 0.
    for users in  get_user_sessions(limit=800*BYTES_IN_MB, remove_autorecurring=True):
        for user in users:
            for (sid, session) in user.sessions.iteritems():
                if session.duration == 0.:
                    zeros += 1
                    if keepzeros:
                        session_lengths.append(session.duration)
                else:
                    session_lengths.append(session.duration)
    session_lengths.sort()
    return session_lengths, zeros

def plot_session_length_histogram(session_lengths, logscale=True):
    if logscale:
        np.seterr(over='raise')
        nz_session_lengths = filter(lambda x: x > 0., session_lengths)
        start = nz_session_lengths[0]
        stop = nz_session_lengths[-1]
        pylab.hist(nz_session_lengths, bins=np.logspace(np.log(start), np.log(stop), NBINS))
        pylab.gca().set_xscale('log')
        pylab.xlabel("Duration (seconds)")
        pylab.ylabel("Number of sessions")
        pylab.show()
    else:
        fig = plt.figure()
        ax = fig.add_subplot(111) # weird, I hate this
        n, bins, patches = ax.hist(session_lengths, NBINS, facecolor="green", alpha=0.75)
        ax.set_xlabel("Duration (seconds)")
        ax.set_ylabel("Number of sessions")
        ax.grid(True)
        plt.show()
   
def plot_session_length_cdf(session_lengths):
    counts, bin_edges = np.histogram(session_lengths, bins=NBINS, normed=True)
    cdf = np.cumsum(counts)
    pylab.plot(bin_edges[1:], cdf)

def write_session_lengths(session_lengths): 
    out = open("session_lengths.txt", 'w')
    for length in session_lengths:
        out.write(str(length) + '\n')
        out.flush()
    out.close()

if __name__ == "__main__":
    main()
