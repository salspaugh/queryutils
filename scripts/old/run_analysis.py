#!/usr/bin/env python

from art.splqueryutils.sessions import *

def main():

    users = get_user_sessions()
    print_basic_stats(users)
    output_entropy(users)

if __name__ == "__main__":
    main()
