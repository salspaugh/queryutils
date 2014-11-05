from splparser import parse as splparse
from splparser.lexers.toplevellexer import tokenize as spltokenize
from .query import *
from logging import getLogger as get_logger

logger = get_logger("queryutils")

def lookup_commands(querystring):
    """Extract the list of commands from the query.

    :param querystring: The query to extract commands from
    :type querystring: str
    :rtype: list
    """
    commands = []
    tokens = tokenize_query(querystring)
    for token in tokens:
        val = token.value.strip().lower()
        if token.type == "EXTERNAL_COMMAND":
            commands.append(val)
        elif token.type == "MACRO":
            commands.append(val)
        elif token.type not in ["ARGS", "PIPE", "LBRACKET", "RBRACKET"]:
            commands.append(val)
    return commands

def extract_schema(query):
    """Return the schema for the query.
    
    This functionality is experimental.

    :param query: The query string for which to extract the schema
    :type query: str
    :rtype: splparser.schema.Schema
    """
    parsetree = parse_query(query)
    if not parsetree is None:
        return parsetree.schema()

def extract_template(query):
    """Return the template for the query.

    :param query: The query string for which to extract the template
    :type query: str
    :rtype: splparser.parsetree.ParseTreeNode
    """
    parsetree = parse_query(query)
    if not parsetree is None:
        return parsetree.template()

def tag_parseable(query):
    """TODO: Delete me.
    """
    query.parseable = False
    query.parsetree = parse_query(query)
    if query.parsetree is not None:
        query.parseable = True

def split_query_into_stages(query):
    """Split the query into stages.

    This takes a query string and returns a list of strings that represent the 
    query stages. This is trickier than just calling split('|') because the
    pipe character is a valid argument to commands if quoted.

    :param query: The query to split
    :type query: str
    :rtype: list
    """
    stages = []
    current_stage = []
    subsearch = []

    for token in tokenize_query(query):

        if token.type == "LBRACKET":
            subsearch.append(token)
            current_stage.append(token.value)
            continue
        
        if token.type == "RBRACKET":
            current_stage.append(token.value)
            subsearch.pop(-1)
            if len(subsearch) == 0:
                stages.append(" ".join(current_stage))
                current_stage = []
            continue

        if len(subsearch) > 0:
            current_stage.append(token.value)
            continue 

        if token.type == "PIPE":
            if len(current_stage) > 0:
                stages.append(" ".join(current_stage))
            current_stage = [token.value]
            continue
            
        current_stage.append(token.value)
   
    if len(current_stage) > 0:
        stages.append(" ".join(current_stage))

    return stages

def parse_query(query):
    """Parse the given query if possible.

    This function is a small wrapper for splparser's parse function that 
    handles unicode issues and logs errors and returns None in the case of
    an error rather than raising an exception.

    :param query: The query to parse
    :type query: str
    :rtype: splparser.parsetree.ParseTreeNode
    """
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
    """TODO: Delete me.
    """
    parsetrees = [parse_query(q) for q in queries]
    return filter(lambda x: x is not None, parsetrees)

def tokenize_query(query):
    """Tokenize the given query if possible.

    This function is a wrapper for splparser's tokenizer that handles
    unicode issues and logs errors and returns None in the case of an
    error rather than raising an exception.

    :param query: The query to tokenize
    :type query: str
    :rtype: list
    """
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
