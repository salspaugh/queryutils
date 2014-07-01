import dateutil.parser
import json
import os
import splparser.parser

from user import *
from query import *

from itertools import chain
from splparser.exceptions import SPLSyntaxError, TerminatingSPLSyntaxError

BYTES_IN_MB = 1048576

def get_users_from_file(filename):
    queries = []
    users = {}
    for result in splunk_result_iter([filename]):
        if 'user' in result and '_time' in result and 'search' in result:
            username = result['user']
            timestamp = float(dateutil.parser.parse(result['_time']).strftime('%s.%f'))
            query_string = unicode(result['search'].strip())
            user = User(username)
            searchtype = result['searchtype']
            query = Query(query_string, timestamp)
            query.user = user
            query.search_type = searchtype
            if not username in users:
                users[username] = user
            users[username].queries.append(query)
    for user in users.values():
        yield user

def get_users_from_directory(limit=50*BYTES_IN_MB):
    raw_data_files = get_json_files(limit=limit)
    for f in raw_data_files:
        for user in get_users_from_file(f):
            yield user

def get_json_files(dir, limit=1000*BYTES_IN_MB):
    json_files = []
    bytes_added = 0.
    for (dirpath, dirnames, filenames) in os.walk(dir):
        for filename in filenames:
            if filename[-5:] == '.json': 
                full_filename = os.path.abspath(dir) + '/' + filename
                json_files.append(full_filename) 
                bytes_added += os.path.getsize(full_filename)
                if bytes_added > limit:
                    return json_files
    return json_files

def put_json_files(iterable, prefix, encoder=json.JSONEncoder, limit=10*BYTES_IN_MB):
    num_files = 0
    filename = prefix + '.' + str(num_files) + '.json'
    out = open(filename, 'w')
    size = os.stat(filename).st_size
    for item in iterable:
        json.dump(item, out, sort_keys=True, indent=4, separators=(',',': '), cls=encoder)
        if size > limit:
            out.close()
            num_files += 1
            filename = prefix + '.' + str(num_files) + '.json'
            out = open(filename, 'w')
        size = os.stat(filename).st_size

def load_data_from_json(jsonfile):
    jsondata = open(jsonfile).read()
    data = json.loads(jsondata)
    return data

def load_and_combine_data_from_json(jsonfiles):
    combined_data = []
    for jsonfile in jsonfiles:
        data = load_data_from_json(jsonfile)
        combined_data.extend(data)
    return combined_data

def print_searches(splunk_results):
    for (key, value) in splunk_result_record_iter(splunk_results):
        if is_search(key):
            print value

def print_parseable_searches(jsonfiles):
    for (key, value) in splunk_result_record_iter(jsonfiles):
        if is_search(key):
            try:
                splparser.parser.parse(unicode(value))
                print value
            except SPLSyntaxError:
                continue
            except TerminatingSPLSyntaxError:
                continue
            except UnicodeEncodeError:
                import sys
                sys.stderr.write("UnicodeEncodeError encountered while parsing: " + value + "\n")
                continue

def splunk_result_iter(jsonfiles):
    for jsonfile in jsonfiles:
        data = load_data_from_json(jsonfile)
        for splunk_result in data:
            yield splunk_result

def splunk_result_record_iter(jsonfiles):
    for jsonfile in jsonfiles:
        data = load_data_from_json(jsonfile)
        for splunk_result in data:
            record_iter = splunk_result.iteritems()
            for (key, value) in record_iter:
                yield (key, value)
 
def is_search(splunk_record_key):
    return splunk_record_key == 'search'

def is_error(splunk_record_key):
    return splunk_record_key == 'error'

def is_search_type(splunk_record_key):
    return splunk_record_key == 'searchtype'

def is_search_length(splunk_record_key):
    return splunk_record_key == 'searchlength'

def is_search_range(splunk_record_key):
    return splunk_record_key == 'range'
