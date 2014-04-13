#!/usr/bin/env python

from art.splqueryutils.sessions import get_user_sessions, UserEncoder
from art.splqueryutils.jsondata import put_json_files, BYTES_IN_MB

def main():
    iter_count = 0
    for users in  get_user_sessions(limit=800*BYTES_IN_MB, remove_autorecurring=True):
        prefix = 'user_sessions.' + str(iter_count)
        put_json_files(users, prefix, encoder=UserEncoder)
        print "Wrote to file prefix " + prefix
        iter_count += 1
        prefix = 'user_sessions.' + str(iter_count)
    #write_user_sessions_to_json(users)

def write_user_sessions_to_json(user_sessions):
    out = open('sessionized_users.json', 'w')
    for user in user_sessions:
        json.dump(user, out, sort_keys=True, indent=4, separators=(',', ': '), cls=UserEncoder)

if __name__ == "__main__":
    main()
