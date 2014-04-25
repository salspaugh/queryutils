
from splparser import parse as splparse
from .query import *
from logging import getLogger as get_logger
import re

ESCAPED_SLASH_STANDIN = "~#$slash$#~"
ESCAPED_SQUOTE_STANDIN = "~#$squote$#~"
ESCAPED_DQUOTE_STANDIN = "~#$dquote$#~"
SQID_MARKER = "~#$sqid$#~"
DQID_MARKER = "~#$dqid$#~"

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
        logger.exception("Failed to parse query: " + text)
        return None
    return parsetree

def parse_queries(queries):
    parsetrees = [parse_query(q) for q in queries]
    return filter(lambda x: x is not None, parsetrees)

def extract_stages_with_cmd(cmd, query):
    stages = break_into_stages(query)
    return filter_stages_by(cmd, stages)

def break_into_stages(query):
    
    try:
        query = query.text.strip()
    except:
        pass
    query.encode('string-escape')
    
    tmp = take_out_escaped_slashes(query)
    tmp = take_out_escaped_single_quotes(tmp)
    tmp = take_out_escaped_double_quotes(tmp)
    tmp, squotes_map = take_out_single_quoted_strings(tmp) 
    tmp, dquotes_map = take_out_double_quoted_strings(tmp)
    
    stages = tmp.split('|') 
    new_stages = []
    for stage in stages:
        stage = stage.strip()
        stage = put_in_mapped_strings(stage, dquotes_map)
        stage = put_in_mapped_strings(stage, squotes_map)
        stage = put_in_escaped_double_quotes(stage)
        stage = put_in_escaped_single_quotes(stage)
        stage = put_in_escaped_slashes(stage)
        new_stages.append(stage)

    return [s.strip() for s in new_stages]

def take_out_escaped_slashes(s):
    return s.replace(r'\\', ESCAPED_SLASH_STANDIN)

def take_out_escaped_single_quotes(s):
    return s.replace(r'\"', ESCAPED_DQUOTE_STANDIN)

def take_out_escaped_double_quotes(s):
    return s.replace(r"\'", ESCAPED_SQUOTE_STANDIN)

def take_out_double_quoted_strings(s):
    quotes = re.findall(r'"[^"]*"', s)
    ids = [DQID_MARKER + str(id) + DQID_MARKER for id in range(len(quotes))]
    quote_map = dict(zip(ids, quotes))
    for (id, quote) in quote_map.iteritems():
        s = s.replace(quote, id)
    return s, quote_map

def take_out_single_quoted_strings(s):
    quotes = re.findall(r"'[^']*'", s)
    ids = [SQID_MARKER + str(id) + SQID_MARKER for id in range(len(quotes))]
    quote_map = dict(zip(ids, quotes))
    for (id, quote) in quote_map.iteritems():
        s = s.replace(quote, id)
    return s, quote_map

def put_in_mapped_strings(s, map):
    for (k,v) in map.iteritems():
        if k in s:
            s = s.replace(k, v)
    return s

def put_in_escaped_slashes(s):
    return s.replace(ESCAPED_SLASH_STANDIN, r'\\')

def put_in_escaped_single_quotes(s):
    return s.replace(ESCAPED_DQUOTE_STANDIN, r'\"')

def put_in_escaped_double_quotes(s):
    return s.replace(ESCAPED_SQUOTE_STANDIN, r"\'")

def filter_stages_by(cmd, stages):
    return filter(lambda x: x.find(cmd) == 0, stages)
