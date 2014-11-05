from collections import defaultdict
from logging import getLogger as get_logger
from queryutils.query import QueryGroup
from queryutils.session import Session

logger = get_logger("queryutils")

NEW_SESSION_THRESH_SECS = 30. * 60.

class DataSource(object):
    """Represents a source of Splunk queries, users, and other data.

    This object should not be initialized directly, rather, one of
    its subclasses should be used.
    """
    
    def __init__(self):
        logger.debug("Initialized data source.")

    def connect(self):
        """Returns a database connection.
        """
        raise NotImplementedError()

    def close(self):
        """Closes a database connection.
        """
        raise NotImplementedError()

    def commit(self):
        """Commits a database transaction.
        """
        raise NotImplementedError()

    def get_users(self):
        """Returns a generator over a set of User objects.

        The User objects returned are not guaranteed to be returned with their
        corresponding queries and sessions.
        If you need users with their corresponding sessions or queries, use a 
        different method.
        """
        raise NotImplementedError()
        
    def get_queries(self, parsed=False):
        """Returns a generator over a set of Query objects without users or sessions.

        Returns only parsed queries if `parsed` is True (defaults to False).
        """
        raise NotImplementedError()

    def get_interactive_queries(self, parsed=False):
        """Returns a generator over the set of interactive Query objects without users or sessions. 

        Returns only parsed queries if `parsed` is True (defaults to False).
        """
        raise NotImplementedError()

    def get_users_with_queries(self, parsed=False): # : Figure out semantics of `parsed`.
        """Returns a generator over a set of Users with queries.

        
        """
        raise NotImplementedError()

    def get_users_with_sessions(self, parsed=False):
        """Returns a generator over a set of Users with queries and sessions.
        """
        raise NotImplementedError()
    
    def extract_sessions_from_user(self, user, remove_suspicious=True):
        """Extract sessions from the given users' queries.

        :param self: The current source object
        :type self: queryutils.DataSource 
        :param user: The user whose queries to sessionize
        :type user: queryutils.User
        :param remove_suspicious: Whether or not to remove queries labeled suspicious
        :type remove_suspicious: bool
        :rtype: None
        """
        if len(user.interactive_queries) == 0: return
        session_id = 0
        user.interactive_queries.sort(key=lambda x: x.time)
        to_sessionize = [q for q in user.interactive_queries 
            if not remove_suspicious or not q.is_suspicious]
        if len(to_sessionize) == 0: return
        prev_time = curr_time = -1.
        session = Session(session_id, user)
        user.sessions[session_id] = session
        for query in to_sessionize:
            curr_time = query.time
            if prev_time < 0.:
                prev_time = curr_time
            query.delta = curr_time - prev_time
            if query.delta > NEW_SESSION_THRESH_SECS:
                self._update_session_duration(user.sessions[session_id])
                session_id += 1
                session = Session(session_id, user)
                user.sessions[session_id] = session
            prev_time = curr_time
            query.session = user.sessions[session_id]
            user.sessions[session_id].queries.append(query)
        self._update_session_duration(user.sessions[session_id])

    def _update_session_duration(self, session):
        first_query = session.queries[0]
        last_query = session.queries[-1]
        session.duration = last_query.time - first_query.time

    def get_query_groups(self, multiple=True):
        # TODO: Delete me?
        #users = { user.id: user for users in self.get_users() }
        iter = 0
        for query in self.get_interactive_queries():
            #query.user = users[query.user_id]
            query_group = QueryGroup(query)
            query_group.id = query.id
            query_group.copies = [q for q in self.get_interactive_queries_with_text(query.text)]
            if len(query_group.copies) <= 1 and multiple:
                logger.debug("Query has no copies.")
                continue
            yield query_group
            if iter % 10 == 0:
                logger.debug("Returned %d query groups." % iter)
            iter += 1
            #if iter > 2000: break
            #for copy in query.copies:
            #    copy.user = users[copy.user_id]
            
        #groups = defaultdict(list)
        #for user in self.get_users_with_queries():
        #    for query in user.queries:
        #        groups[query.text].append(query)
        #for (text, group) in groups.iteritems():
        #    for query in group:
        #        qg = QueryGroup(query)
        #        qg.copies = group
        #        yield qg

    def extract_command_stage(self, parsetree, commands):
        """Extract the subtrees of the given parsetree that have one of the given commands.
        
        :param self: The current source object
        :type self: queryutils.DataSource 
        :param parsetree: The given parsetree from which to extract "STAGE" subtrees
        :type parsetree: splparser.ParseTreeNode
        :param commands: The list of commands to match against
        :type commands: list
        :rtype: generator
        """
        if parsetree is not None:
            count = -1
            for node in parsetree.itertree():
                if node.role == "STAGE":
                    count += 1
                    if (len(commands) == 0 and node.children[0].role == "COMMAND") or \
                        (node.children[0].role == "COMMAND" and node.children[0].raw in commands):
                        yield node, count

    def get_unique_stages(self, commands):
        """Return a generator over unique stages whose command matches one of the given types.

        :param self: The current source object
        :type self: queryutils.DataSource 
        :param commands: The list of commands to match against
        :type commands: list
        :rtype: generator
        """
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
                logger.debug("Returned %d unique stages." % iter)
            iter += 1

    def get_unique_filters(self):
        """Return all unique stages that are "Filter" types.
        
        :param self: The current source object
        :type self: queryutils.DataSource 
        :rtype: generator
        """
        # TODO: Reimplement using splunktypes lookup instead of list!
        filters = ["dedup", "head", "regex", "search", "tail", "where", "uniq"]
        for filter in self.get_unique_stages(filters):
            yield filter

    def get_unique_aggregates(self):
        """Return all unique stages that are "Aggregate" types.
        
        :param self: The current source object
        :type self: queryutils.DataSource 
        :rtype: generator
        """
        # TODO: Reimplement using splunktypes lookup instead of list!
        aggregates = ["addtotals", "addcoltotals", "chart", "eventcount", "geostats", 
            "mvcombine", "rare", "stats", "timechart", "top", "transaction", "tstats"]
        for aggregate in self.get_unique_stages(aggregates):
            yield aggregate

    def get_unique_augments(self):
        """Return all unique stages that are "Augment" types.

        :param self: The current source object
        :type self: queryutils.DataSource 
        :rtype: generator
        """
        # TODO: Reimplement using splunktypes lookup instead of list!
        augments = ["addinfo", "addtotals", "appendcols", "bin", "bucket", 
            "eval", "eventstats", "extract", "gauge", "iplocation", "kv", "outputtext",
            "rangemap", "relevancy", "rex", "spath", "strcat", "tags", "xmlkv"]
        for augment in self.get_unique_stages(augments):
            yield augment
