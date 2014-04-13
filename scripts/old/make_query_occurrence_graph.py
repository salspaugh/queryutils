#!/usr/bin/env python

from art.splqueryutils.sessions import get_user_sessions, UserEncoder
from art.splqueryutils.jsondata import put_json_files, BYTES_IN_MB
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pylab

NBINS = 1000.

def main():
    counts = compute_query_occurrences()
    print "Done computing query occurrences."
    plot_query_occurrences_graph(counts)

def compute_query_occurrences():
    queries = defaultdict(int)
    for users in  get_user_sessions(limit=800*BYTES_IN_MB, remove_autorecurring=False):
        for user in users:
            for query in user.queries:
                queries[query.text] += 1
    counts = queries.values()
    counts.sort(reverse=True)
    return counts

def plot_query_occurrences_graph(counts, log=True):
    rank = range(1, len(counts)+1)
    if log:
        counts = map(lambda x: np.log(x), counts)
        rank = map(lambda x: np.log(x), rank)
    fig = plt.figure()
    ax = fig.add_subplot(111) # weird, I hate this
    line = plt.plot(rank, counts)
    ax.set_xlabel("Rank")
    ax.set_ylabel("Number of occurrences")
    if log:
        ax.set_xlabel("Rank (log-scale)")
        ax.set_ylabel("Number of occurrences (log-scale)")
    plt.setp(line, color='r', linewidth=2.0)
    plt.show()

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
        #pylab.set_xlabel("Duration (seconds)")
        #pylab.set_ylabel("Number of sessions")
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

if __name__ == "__main__":
    main()
