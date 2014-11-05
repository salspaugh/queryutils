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
    """Return a generator over user objects and their queries from the given file.

    It is assumed that the file will contain a list of results in JSON format.
    Each result is a dictionary with an assumed set of keys.
    This is the format that corresponds to Version.FORMAT_2012.

    :param filename: The path to the .json file containing the queries
    :type filename: str
    :rtype: generator
    """
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
    # TODO: FIXME: This might cause incorrectness if a user's queries span files. 
    # For the data we have, this is not an issue, since cases never span files.
    for user in users.values():
        yield user

def get_users_from_directory(limit=50*BYTES_IN_MB):
    """Return a generator over user objects and their queries from the given directory.

    The directory is assumed to contain a list of .json files, each of which 
    is assumed to adhere to the format expected by get_users_from_file.

    :param limit: The approximate number of bytes to read in (for testing)
    :type limit: int
    :rtype: generator
    """
    raw_data_files = get_json_files(limit=limit)
    for f in raw_data_files:
        for user in get_users_from_file(f):
            yield user

def get_json_files(dir, limit=1000*BYTES_IN_MB):
    """Return a list of the full paths to each of the .json files in a directory.

    :param dir: The path to the directory to check
    :type dir: str
    :param limit: The approximate number of bytes to read in (for testing)
    :type limit: int
    :rtype: list
    """
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
    """Write out a list of .json files with the data in the given iterable.

    TODO: Delete me.

    :param iterable: A list or other iterable containing items to encode
    :type iterable: iterable
    :param prefix: The prefix to name each of the files with (files will be named e.g., prefix.1, prefix.2, etc.)
    :type prefix: str
    :param encoder: The encoder object that knows how to encode the items in iterable
    :type encoder: json.JSONEncoder
    :param limit: The approximate number of bytes to read in (for testing)
    :type limit: int
    """
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
    """Load the data contained in a .json file and return the corresponding Python object.

    :param jsonfile: The path to the .json file
    :type jsonfile: str
    :rtype: list or dict
    """
    jsondata = open(jsonfile).read()
    data = json.loads(jsondata)
    return data

def load_and_combine_data_from_json(jsonfiles):
    """Return all the data in a list of .json files in a big list.

    :param jsonfile: The path to the .json file
    :type jsonfile: str
    :rtype: list or dict
    """
    combined_data = []
    for jsonfile in jsonfiles:
        data = load_data_from_json(jsonfile)
        combined_data.extend(data)
    return combined_data

def print_searches(splunk_results):
    """Print all the searches contained in a set of files containing Splunk results.

    :param splunk_results: A list of .json files
    :type splunk_results: list
    :rtype: None
    """
    for (key, value) in splunk_result_record_iter(splunk_results):
        if is_search(key):
            print value

def print_parseable_searches(jsonfiles):
    """Print all the parseable searches contained in a set of files containing Splunk results.
    
    :param jsonfiles: A list of .json files
    :type jsonfiles: list
    :rtype: None
    """
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
    """Yield the Splunk result dicts from the given .json file.
    
    :param jsonfiles: A list of .json files
    :type jsonfiles: list
    :rtype: generator
    """
    for jsonfile in jsonfiles:
        data = load_data_from_json(jsonfile)
        for splunk_result in data:
            yield splunk_result

def splunk_result_record_iter(jsonfiles):
    """Yield the Splunk result dicts from the list of .json files.
    
    :param jsonfiles: A list of .json files
    :type jsonfiles: list
    :rtype: generator
    """
    for jsonfile in jsonfiles:
        data = load_data_from_json(jsonfile)
        for splunk_result in data:
            record_iter = splunk_result.iteritems()
            for (key, value) in record_iter:
                yield (key, value)
 
def is_search(splunk_record_key):
    """Return True if the given string is a search key.

    :param splunk_record key: The string to check
    :type splunk_record_key: str
    :rtype: bool
    """
    return splunk_record_key == 'search'

def is_error(splunk_record_key):
    """Return True if the given string is an error key.

    :param splunk_record key: The string to check
    :type splunk_record_key: str
    :rtype: bool
    """
    return splunk_record_key == 'error'

def is_search_type(splunk_record_key):
    """Return True if the given string is a search type key.

    :param splunk_record key: The string to check
    :type splunk_record_key: str
    :rtype: bool
    """
    return splunk_record_key == 'searchtype'

def is_search_length(splunk_record_key):
    """Return True if the given string is a search length key.

    :param splunk_record key: The string to check
    :type splunk_record_key: str
    :rtype: bool
    """
    return splunk_record_key == 'searchlength'

def is_search_range(splunk_record_key):
    """Return True if the given string is a range key.

    :param splunk_record key: The string to check
    :type splunk_record_key: str
    :rtype: bool
    """
    return splunk_record_key == 'range'
