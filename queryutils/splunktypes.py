from splparser.parsetree import ParseTreeNode
from parse import tokenize_query

category = {
    "abstract": "Miscellaneous",
    "addcoltotals": "Aggregate",
    "addinfo": "Augment",
    "addtotals col": "Aggregate",
    "addtotals row": "Augment",
    "addtotals": "Augment",
    "anomalies": "Window",
    "append": "Set",
    "appendcols": "Augment",
    "appendpipe": "Set",
    "audit": "Read Metadata",
    "bin": "Augment", # alias for bucket
    "bucket": "Augment",
    "chart": "Aggregate",
    "collect": "Cache",
    "convert": "Transform",
    "contingency": "Aggregate",
    "counttable": "Aggregate",
    "datamodel": "Input",
    "dbinspect": "Read Metadata",
    "dbquery": "Input",
    "dedup": "Filter",
    "delete": "Meta",
    "delta": "Window",
    "eval": "Augment",
    "eventcount": "Aggregate",
    "eventstats": "Augment", # aggregation and projection
    "export": "Output",
    "extract": "Augment",
    "fieldformat": "Transform",
    "fields": "Project",
    "filldown": "Transform",
    "fillnull": "Transform",
    "format": "Miscellaneous",
    "gauge": "Augment",
    "geostats": "Aggregate",
    "geoip": "Join",
    "head": "Filter",
    "history": "Read Metadata",
    "inputcsv": "Input",
    "inputlookup": "Input",
    "iplocation": "Augment",
    "join": "Join",
    "kv": "Augment",
    "loadjob": "Input",
    "localop": "Meta",
    "lookup": "Join",
    "macro": "Macro",
    "makemv": "Transform",
    "map": "Miscellaneous",
    "metadata": "Input",
    "metasearch": "Meta",
    "multikv": "Transform",
    "mvcombine": "Aggregate",
    "mvexpand": "Transform",
    "nomv": "Transform",
    "noop": "Miscellaneous",
    "outlier": "Transform",
    "outputcsv": "Output",
    "outputlookup": "Output",
    "outputtext": "Augment",
    "overlap": "Miscellaneous",
    "rangemap": "Augment",
    "rare": "Aggregate",
    "regex": "Filter",
    "relevancy": "Augment",
    "rename": "Rename",
    "replace": "Transform",
    "rest": "Input",
    "return": "Miscellaneous",
    "reverse": "Reorder",
    "rex": "Augment",
    "savedsearch": "Input",
    "search": "Filter",
    "sendemail": "Output",
    "set": "Set",
    "sichart": "Cache",
    "sirare": "Cache",
    "sistats": "Cache",
    "sitimechart": "Cache",
    "sitop": "Cache",
    "sort": "Reorder",
    "spath": "Augment",
    "stats": "Aggregate",
    "strcat": "Augment",
    "streamstats": "Window",
    "summaryindex": "Cache",
    "summarize": "Cache",
    "table": "Project",
    "tags": "Augment",
    "tail": "Filter",
    "timechart": "Aggregate",
    "timewrap": "Aggregate",
    "top": "Aggregate",
    "transaction": "Aggregate",
    "transpose": "Transpose",
    "tscollect": "Cache",
    "tstats": "Aggregate",
    "typeahead": "Read Metadata",
    "uniq": "Filter",
    "where": "Filter",
    "xmlkv": "Augment",
}

def lookup_categories(querystring):
    tokens = tokenize_query(querystring)
    categories = []
    for idx, token in enumerate(tokens):
        if token.type == "EXTERNAL_COMMAND":
            categories.append(category.get(token.value, "Miscellaneous"))
        elif token.type == "MACRO":
            categories.append("Macro")
        elif token.type not in ["ARGS", "PIPE", "LBRACKET", "RBRACKET"]:
            command = token.value.lower()
            # Note: This is an imperfect way to detect this.
            # See below for an example.
            if token.value == "addtotals": 
                if len(tokens) == idx+1:
                    command = "addtotals row"
                elif tokens[idx+1].value.lower()[:3] == "row":
                    command = "addtotals row"
                else:
                    command = "addtotals col"
            try:
                categories.append(lookup_category(command))
            except KeyError as e:
                print e, token
    return categories


def lookup_category(node_or_string):
    if not isinstance(node_or_string, ParseTreeNode):
        return category[node_or_string]
    command = node_or_string.children[0].raw
    if command == "addtotals":
        command = detect_addtotals_type(node_or_string)
    return category[command]


def detect_addtotals_type(stagenode):
    optionnodes = []
    for node in stagenode.itertree():
        if node.role == "EQ" and node.children[0].role == "OPTION":
            optionnodes.append(node)
    for optionnode in optionnodes:
        paramnode = optionnode.children[0]
        valuenode = optionnode.children[1]
        value = detect_truth_value(valuenode.raw)
        if value and paramnode.raw == "col":
            return "addtotals col"
        if value and paramnode.raw == "row":
            return "addtotals row"
    return "addtotals row"

    
def detect_truth_value(astring):
    value = False
    if astring.lower() in ["true", "t"]:
        value = True
    else:   
        try:
            value = float(valuenode.raw)
        except:
            pass
    return value
