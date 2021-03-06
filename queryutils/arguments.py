from queryutils.databases import PostgresDB, SQLite3DB
from queryutils.files import CSVFiles, JSONFiles

SOURCES = {
    "csvfiles": (CSVFiles, ["path", "version"]),
    "jsonfiles": (JSONFiles, ["path", "version"]),
    "postgresdb": (PostgresDB, ["database", "user", "password"]),
    "sqlite3db": (SQLite3DB, ["srcpath"])
}

def initialize_source(source, args):
    src_class = SOURCES[source][0]
    src_args = lookup(vars(args), SOURCES[source][1])
    source = src_class(*src_args)
    return source

def lookup(dictionary, lookup_keys):
    return [dictionary[k] for k in lookup_keys]

def get_arguments(parser, o=False, w=False):
  parser.add_argument("-s", "--source",
                      help="one of: " + ", ".join(SOURCES.keys()))
  parser.add_argument("-a", "--path",
                      help="the path to the data to load")
  parser.add_argument("-v", "--version", #TODO: Print possible versions
                      help="the version of data collected")
  parser.add_argument("-U", "--user",
                      help="the user name for the Postgres database")
  parser.add_argument("-P", "--password",
                      help="the password for the Postgres database")
  parser.add_argument("-D", "--database",
                      help="the database for Postgres")
  parser.add_argument("-q", "--querytype",
                      help="the type of queries (scheduled or interactive)")

  if o:
    parser.add_argument("-o", "--output",
                      help="the name of the output file")
  if w:
    parser.add_argument("-w", "--weighted", action="store_true",
                      help="if true, average across users")

  return parser.parse_args()
