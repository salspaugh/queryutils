from splparser import parse as splparse
from splparser.lexers.toplevellexer import tokenize as spltokenize
from .query import *
from logging import getLogger as get_logger

logger = get_logger("queryutils")

def extract_schema(query):
    parsetree = parse_query(query)
    if not parsetree is None:
        return parsetree.schema()

def extract_template(query):
    parsetree = parse_query(query)
    if not parsetree is None:
        return parsetree.template()

def tag_parseable(query):
    query.parseable = False
    query.parsetree = parse_query(query)
    if query.parsetree is not None:
        query.parseable = True

def parse_query(query):
    text = ""
    if isinstance(query, Query):
        q = str(query.text.encode("utf8")) if type(query.text) == unicode else str(query.text.decode("utf8").encode("utf8"))
    else:
        q = str(query.encode("utf8")) if type(query) == unicode else str(query.decode("utf8").encode("utf8"))
    try:
        parsetree = splparse(q)
    except:
        logger.exception("Failed to parse query: " + q)
        return None
    return parsetree

def parse_queries(queries):
    parsetrees = [parse_query(q) for q in queries]
    return filter(lambda x: x is not None, parsetrees)

def tokenize_query(query):
    text = ""
    if isinstance(query, Query):
        q = str(query.text.encode("utf8")) if type(query.text) == unicode else str(query.text.decode("utf8").encode("utf8"))
    else:
        q = str(query.encode("utf8")) if type(query) == unicode else str(query.decode("utf8").encode("utf8"))
    try:
        tokens = spltokenize(q) 
    except:
        logger.exception("Failed to tokenize query: " + q)
        return None
    return tokens
