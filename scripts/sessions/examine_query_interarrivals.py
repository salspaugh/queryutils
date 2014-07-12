#!/usr/bin/env python

from queryutils.databases import PostgresDB, SQLite3DB
from queryutils.files import CSVFiles, JSONFiles
from numpy import histogram
import matplotlib.pyplot as plt

SOURCES = {
    "csvfiles": (CSVFiles, ["srcpath", "version"]),
    "jsonfiles": (JSONFiles, ["srcpath", "version"]),
    "postgresdb": (PostgresDB, ["database", "user", "password"]),
    "sqlite3db": (SQLite3DB, ["srcpath"])
}

def main(src, args):
    src_class = SOURCES[src][0]
    src_args = lookup(args, SOURCES[src][1])
    source = src_class(*src_args)
    plot_interarrival_histogram(source)

def lookup(map, keys):
    return [map[k] for k in keys]

def plot_interarrival_histogram(src):
    data = []
    for user in src.get_users_with_queries():
        if user.name == "splunk-system-user": continue
        interarrivals = compute_interarrivals(user.interactive_queries) 
        data += interarrivals
    print "Total intervals computed: %d" % len(data)
    #to_plot = [x for x in data if x <= 14400.]
    #print "Discarded %d values over 4 hours" % (len(data) - len(to_plot))
    #to_plot = [x for x in data if x <= 2700.]
    #print "Discarded %d values over 45 minutes" % (len(data) - len(to_plot))
    to_plot = [x for x in data if x <= 30.]
    print "Discarded %d values over 30 seconds" % (len(data) - len(to_plot))
    too_short = [x for x in data if x < 1.]
    print len(too_short), too_short
    hist, edges = histogram(to_plot, bins=1000) 
    fig = plt.figure()
    rects = plt.bar(edges[:-1], hist, log=True)
    plt.show()

def compute_interarrivals(queries):
    queries.sort(key=lambda x: x.time)
    queries = [x for x in queries if not x.is_suspicious]
    interarrivals = []
    for idx, query in enumerate(queries[:-1]):
        interarrivals.append(queries[idx+1].time - query.time)
    return interarrivals

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser("Load data from SPL query logs into the database.")
    parser.add_argument("-s", "--source",
                        help="one of: " + ", ".join(SOURCES.keys()))
    parser.add_argument("-p", "--srcpath",
                        help="the path to the data to load")
    parser.add_argument("-v", "--version", #TODO: Print possible versions 
                        help="the version of data collected")
    parser.add_argument("-u", "--user",
                        help="the user name for the Postgres database")
    parser.add_argument("-w", "--password",
                        help="the password for the Postgres database")
    parser.add_argument("-b", "--database",
                        help="the database for Postgres")
    args = parser.parse_args()
    main(args.source, vars(args))
