from splparser.parsetree import ParseTreeNode


category = {
    "abstract": "Miscellaneous",
    "addcoltotals": "Aggregate",
    "addinfo": "Augment",
    "addtotals col": "Aggregate",
    "addtotals row": "Augment",
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
    "datamodel": "Input",
    "dbinspect": "Read Metadata",
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
    "gauge": "Augment",
    "geostats": "Aggregate",
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
    "mvexpand": "Input",
    "nomv": "Transform",
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
    "table": "Project",
    "tags": "Augment",
    "tail": "Filter",
    "timechart": "Aggregate",
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


def lookup_category(stagenode):
    if not isinstance(stagenode, ParseTreeNode):
        return category[stagenode]
    command = stagenode.children[0].raw
    if command == "addtotals":
        command = detect_addtotals_type(stagenode)
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
