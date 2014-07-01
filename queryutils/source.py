from logging import getLogger as get_logger
from queryutils.query import Text

logger = get_logger("queryutils")

class DataSource(object):
    
    def __init__(self):
        pass

    def connect(self):
        """
        Returns a database connection.
        """
        raise NotImplementedError()

    def close(self):
        """
        Closes a database connection.
        """
        raise NotImplementedError()

    def commit(self):
        """
        Commits a database transaction.
        """
        raise NotImplementedError()

    def get_users(self):
        """
        Returns a generator over a set of User objects.
        The User objects returned are not guaranteed to be returned with their
        corresponding queries and sessions.
        If you need users with their corresponding sessions or queries, use a 
        different method.
        """
        raise NotImplementedError()
        
    def get_queries(self, parsed=False):
        """
        Returns a generator over a set of Query objects without users or sessions.
        Returns only parsed queries if `parsed` is True (defaults to False).
        """
        raise NotImplementedError()

    def get_users_with_queries(self, parsed=False): # TODO: Figure out semantics of `parsed`.
        """
        Returns a generator over a set of Users with queries.
        Users returned are not guaranteed to have sessionized queries.
        """
        raise NotImplementedError()

    def get_users_with_sessions(self, parsed=False):
        """
        Returns a generator over a set of Users with queries and sessions.
        """
        raise NotImplementedError()

    def get_interactive_queries(self, parsed=False):
        """
        Returns a generator over the set of interactive Query objects without users or sessions. 
        Returns only parsed queries if `parsed` is True (defaults to False).
        """
        raise NotImplementedError()

    def extract_command_stage(self, parsetree, commands):
        if parsetree is not None:
            count = -1
            for node in parsetree.itertree():
                if node.role == "STAGE":
                    count += 1
                    if (len(commands) == 0 and node.children[0].role == "COMMAND") or \
                        (node.children[0].role == "COMMAND" and node.children[0].raw in commands):
                        yield node, count

    def get_possibly_suspicious_texts(self):
        groups = {}
        for query in self.get_interactive_queries():
            if not query.text in groups:
                groups[query.text] = {}
            if not query.user_id in groups[query.text]:
                groups[query.text][query.user_id] = []
            groups[query.text][query.user_id].append(query)
        texts = []
        for (stmt, users) in groups.iteritems():
            for (uid, queries) in users.iteritems():
                text = Text(stmt, uid)
                text.all_occurrences = queries
                text.all_users = users.keys()
                text.selected_occurrences = [q for q in queries if q.user_id == uid]
                text.id = len(texts)
                texts.append(text)
        for text in texts:
            if len(text.all_occurrences) == 1:
                continue
            text.interarrivals = text.get_interarrivals()
            yield text

    def get_unique_stages(self, commands):
        seen = set()
        iter = 0
        for parsetree in self.get_parsetrees():
            counter = 0
            for (stage, pos) in self.extract_command_stage(parsetree, commands):
                stage_key = stage.str_tree()
                if stage_key in seen:
                    continue
                seen.add(stage_key)
                stage.id = ".".join([str(parsetree.query_id), str(counter)])
                stage.position = pos
                counter += 1
                yield stage
            if iter % 10 == 0:
                logger.debug("[data] - Returned %d objects." % iter)
            iter += 1

    def get_unique_filters(self):
        filters = ["dedup", "head", "regex", "search", "tail", "where", "uniq"]
        for filter in self.get_unique_stages(filters):
            yield filter

    def get_unique_aggregates(self):
        aggregates = ["addtotals", "addcoltotals", "chart", "eventcount", "geostats", 
            "mvcombine", "rare", "stats", "timechart", "top", "transaction", "tstats"]
        for aggregate in self.get_unique_stages(aggregates):
            yield aggregate

    def get_unique_augments(self):
        augments = ["addinfo", "addtotals", "appendcols", "bin", "bucket", 
            "eval", "eventstats", "extract", "gauge", "iplocation", "kv", "outputtext",
            "rangemap", "relevancy", "rex", "spath", "strcat", "tags", "xmlkv"]
        for augment in self.get_unique_stages(augments):
            yield augment
