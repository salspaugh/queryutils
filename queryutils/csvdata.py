
import csv
import dateutil.parser
import os
import splparser.parser

from user import *
from query import *

from logging import getLogger as get_logger
from os import path
from splparser.exceptions import SPLSyntaxError, TerminatingSPLSyntaxError


BYTES_IN_MB = 1048576
LIMIT = 2000*BYTES_IN_MB

logger = get_logger("queryutils")


def get_users_from_file(filename):
    logger.debug("Reading from file:" + filename)
    first = True
    users = {}
    with open(filename) as datafile:
        reader = csv.DictReader(datafile)
        for row in reader:
            logger.debug("Attempting to read row.")
            username = row.get('user', None)
            if username is not None:
                username = unicode(username.decode("utf-8"))

            case = row.get('case_id', None)
            if case is not None:
                case = unicode(case.decode("utf-8"))
            if case is not None:
                username = ".".join([username, case])

            user = User(username)
            user.case = case

            timestamp = float(dateutil.parser.parse(row.get('_time', None)).strftime('%s.%f'))

            search = row.get('search', None)
            if search is not None:
                query_string = unicode(search.decode("utf-8")).strip()

            searchtype = row.get('searchtype', None)
            if searchtype is None:
                searchtype = row.get('search_type', None)
            if searchtype is not None:
                searchtype = unicode(searchtype.decode("utf-8"))

            search_et = row.get('search_et', None)
            if search_et is not None:
                try:
                    search_et = float(search_et.decode("utf-8"))
                except:
                    search_et = None

            search_lt = row.get('search_lt', None)
            if search_lt is not None:
                try:
                    search_lt = float(search_lt.decode("utf-8"))
                except:
                    search_lt = None

            range = row.get('range', None)
            if range is not None:
                try:
                    range = float(range.decode("utf-8"))
                except:
                    range = None

            is_realtime = row.get('is_realtime', None)
            if is_realtime is not None and is_realtime == "false":
                is_realtime = False
            if is_realtime is not None and is_realtime == "true":
                is_realtime = True

            splunk_id = row.get('search_id', None)
            if splunk_id is not None:
                splunk_id = unicode(splunk_id.decode("utf-8"))

            runtime = row.get('runtime', None)
            if runtime is None:
                runtime = row.get('total_run_time', None)
            if runtime is not None:
                try:
                    runtime = float(runtime.decode("utf-8"))
                except:
                    runtime = None

            savedsearch_name = row.get('savedsearch_name', None)
            if savedsearch_name is not None:
                savedsearch_name = unicode(savedsearch_name.decode("utf-8"))

            query = Query(query_string, timestamp, user, searchtype, 
                            search_et, search_lt, range, 
                            is_realtime, splunk_id, runtime, savedsearch_name) 
            
            if not username in users:
                users[username] = user
            users[username].queries.append(query)
            logger.debug("Successfully read query.")
    return users.values()

def get_users_from_directory(directory, limit=LIMIT):
    raw_data_files = get_csv_files(directory, limit=limit)
    for f in raw_data_files:
        yield get_users_from_file(f)

def get_csv_files(dir, limit=LIMIT):
    csv_files = []
    bytes_added = 0.
    for (dirpath, dirnames, filenames) in os.walk(dir):
        for filename in filenames:
            if filename[-4:] == '.csv': 
                full_filename = path.join(path.abspath(dir), filename)
                csv_files.append(full_filename) 
                bytes_added += path.getsize(full_filename)
                if bytes_added > limit:
                    return csv_files
    return csv_files
