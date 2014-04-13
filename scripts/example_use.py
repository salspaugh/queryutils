

from argparse import ArgumentParser
from queryutils import get_user_sessions

def print_sessions_from_file(filename, version=None):
    for users in get_user_sessions(filename=filename):
        for user in users:
            for id, session in user.sessions.iteritems():
                print id, session

def print_sessions_from_directory(directory, version=None):
    for users in get_user_sessions(directory=directory):
        for user in users:
            for id, session in user.sessions.iteritems():
                print user.name, id, session

if __name__ == "__main__":

    parser = ArgumentParser("Demonstrates how to extract sessions from SPL query logs using the queryutils module.")
    parser.add_argument("-f", "--filename", # TODO: Accept a list of filenames instead.
                        help="A query log containing SPL query data. Specify this OR directory.")
    parser.add_argument("-d", "--directory",
                        help="A directory containing SPL query logs. Specify this OR filename.")
    parser.add_argument("-q", "--logversion", # TODO: Explain what version types are available.
                        help="The version of the query logs -- the format of the logs is dependent upon this.")

    args = parser.parse_args()

    if args.filename:
        print "Reading queries from file."
        if args.logversion:
            print_sessions_from_file(args.filename, version=args.logversion)
        else:
            print_sessions_from_file(args.filename)
    elif args.directory:
        print "Reading queries from directory."
        if args.logversion:
            print_sessions_from_directory(args.directory, version=args.logversion)
        else:
            print_sessions_from_directory(args.directory)
    else:
        parser.print_help()
