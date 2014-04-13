#!/usr/bin/env python

import queryutils
import sys

from collections import defaultdict

BYTES_IN_MB = 1048576

def main():
    done = 0
    seen = defaultdict(int)
    for queries in queryutils.get_queries(limit=800*BYTES_IN_MB):
        for query in queries:
            template = queryutils.extract_template(query) 
            if template is None: continue
            s = template.str_tree()
            seen[s] += 1 
            done += 1
            if done % 100 == 0:
                sys.stderr.write(str(done) + " done\n")
                sys.stderr.flush()

    print "Number of templates: ", len(seen.keys())
    templates = sorted(seen.items(), key=lambda x: x[1], reverse=True)
    for (template, count) in templates:
        print "Template: \n", template
        print "Count: ", count
        print

main()
