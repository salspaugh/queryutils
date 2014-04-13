#!/usr/bin/env python

from art import splqueryutils
from splparser.parser import parse as splparse

BYTES_IN_MB = 1048576

def main(cmd):
    seen = {}
    printed = 0
    for queries in splqueryutils.get_queries(limit=800*BYTES_IN_MB):
        for query in queries:
            if not query.text in seen:
                stages = splqueryutils.break_into_stages(query)
                cmd_invocations = splqueryutils.filter_stages_by(cmd, stages)
                for c in cmd_invocations:
                    try:
                        splparse(c)
                    except Exception as e:
                        print e.message
                        print c
                        printed += 1
            seen[query.text] = 1
    print "Failed command invocations: " + str(printed)

def print_with_macros(q):
    if has_macro(q):
        try:
            sys.stderr.write(q + '\n')
            p = parse(q).print_tree()
            print q
            print p
        except:
            pass

def has_macro(s):
    return s.find("`") > -1

if __name__ == "__main__":
    cmd = 'search'
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-c", "--command", dest="cmd",
                      help="print parseable queries that use CMD ", metavar="CMD")

    (options, args) = parser.parse_args()
    cmd = options.cmd
    main(cmd)

