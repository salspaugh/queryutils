from logging import getLogger as get_logger
from os.path import isfile, isdir
from queryutils.user import User
from queryutils.session import Session
from queryutils.query import Query, Text
from queryutils.parse import parse_query
from splparser.parsetree import ParseTreeNode

import psycopg2
import sqlite3

"""
Functions for getting data from sources:

    get_unique_stages()
    get_unique_filters()
    get_unique_aggregates()
    get_unique_augments()

    get_users()
    get_queries()  
    get_sessions()
    get_parsetrees() 
    
    get_users_with_queries()
    get_users_with_sessions()
   
    get_interactive_queries() * 
    get_possibly_suspicious_texts() *

    * = Partially implemented

TODO: Make sure these all return the same things for each class.
"""

NEW_SESSION_THRESH_SECS = 30. * 60.
REPEAT_THRESH_SECS = 1.

logger = get_logger("queryutils")

SUSPICIOUS_QUERIES = [
    "| metadata type=sourcetypes | search totalCount > 0",
    "|history | head 2000 | search event_count>0 OR result_count>0 | dedup search | table search",
    #"`get_splunk_servers` | fields sos_server server_role _time | inputlookup append=true splunk_servers_cache | `curate_splunk_servers_cache` | outputlookup splunk_servers_cache",
    #"`get_splunk_instances_info` | inputlookup append=true splunk_instances_info | stats first(*) AS * by sos_server | outputlookup splunk_instances_info",
    #'| metadata type=sourcetypes | search totalCount > 0 | table sourcetype totalCount recentTime | fieldformat totalCount=tostring(totalCount, "commas") | fieldformat recentTime=strftime(recentTime, "%Y-%m-%dT%H:%M:%S.%Q%:z")',
    #'| metadata type=hosts | search totalCount > 0 | table host totalCount recentTime | fieldformat totalCount=tostring(totalCount, "commas") | fieldformat recentTime=strftime(recentTime, "%Y-%m-%dT%H:%M:%S.%Q%:z")',
    #'| metadata type=sources | search totalCount > 0 | table source totalCount recentTime | fieldformat totalCount=tostring(totalCount, "commas") | fieldformat recentTime=strftime(recentTime, "%Y-%m-%dT%H:%M:%S.%Q%:z")',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m` ]',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m_by_host` ]',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m_by_sourcetype` ]',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m_by_pool` ]',
    #'typeahead prefix="index" max_time="1" count="50" use_cache=1',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m_by_indexer` ]',
    #'search `dm_index_week_over_week`',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m_by_forwarder` ]',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `sources_summary_10m` ]',
    #'search `dm_tcpin_today_vs_last_week`',
    #'search `dm_index_today_vs_last_week`',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `sourcetypes_summary_10m` ]',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `pools_summary_10m` ]',
    #'summarize override=partial timespan= max_summary_size=52428800 max_summary_ratio=0.1 max_disabled_buckets=2 max_time=3600 [ search `dm_license_summary_10m_by_source` ]',
    #'| rest splunk_server=local /services/licenser/messages | where (category=="license_window" OR category=="pool_over_quota") AND create_time >= now() - (30 * 86400) | rename pool_id AS pool | eval warning_day=if(category=="pool_over_quota","(".strftime(create_time,"%B %e, %Y").")",strftime(create_time-43200,"%B %e, %Y")) | fields pool warning_day | join outer pool [rest splunk_server=local /services/licenser/slaves | mvexpand active_pool_ids | eval slave_name=label | eval pool=active_pool_ids | fields pool slave_name | stats values(slave_name) as "members" by pool] | join outer pool [rest splunk_server=local /services/licenser/pools | eval pool=title | eval quota=if(isnull(effective_quota),quota,effective_quota) | eval quotaGB=round(quota/1024/1024/1024,3) | fields pool stack_id, quotaGB] | stats first(pool) as "Pool" first(stack_id) as "Stack ID" first(members) as "Current Members" first(quotaGB) as "Current Quota (GB)" values(warning_day) AS "Warning Days - (Soft)/Hard" by pool | fields - pool',
    #'search index=network source="/apps/data/network/raw/logs/syslog" "%ILPOWER-7-DETECT" | top hostname limit="40" pgm_group INT_5 | rename INT_5 AS Interface | fields - percent',
    #'| rest splunk_server=local /services/licenser/pools | rename title AS Pool | search [rest splunk_server=local /services/licenser/groups | search is_active=1 | eval stack_id=stack_ids | fields stack_id] | eval quota=if(isnull(effective_quota),quota,effective_quota) | eval Used=round(used_bytes/1024/1024/1024, 3) | eval Quota=round(quota/1024/1024/1024, 3)| fields Pool Used Quota',
    #'| rest splunk_server=local /services/licenser/pools | rename title AS Pool | search [rest splunk_server=local /services/licenser/groups | search is_active=1 | eval stack_id=stack_ids | fields stack_id] | eval quota=if(isnull(effective_quota),quota,effective_quota) | eval "% used"=round(used_bytes/quota*100,2) | fields Pool "% used"',
    #'| rest splunk_server=local /services/licenser/pools | rename title AS pool | search [rest splunk_server=local /services/licenser/groups | search is_active=1 | eval stack_id=stack_ids | fields stack_id] | eval name=pool | eval value="pool=". pool | table name value',
    #'| rest splunk_server=local /services/licenser/pools | rename title AS Pool | search [rest splunk_server=local /services/licenser/groups | search is_active=1 | eval stack_id=stack_ids | fields stack_id] | join type=outer stack_id [rest splunk_server=local /services/licenser/stacks | eval stack_id=title | eval stack_quota=quota | fields stack_id stack_quota] | stats sum(used_bytes) as used max(stack_quota) as total | eval usedGB=round(used/1024/1024/1024,3) | eval totalGB=round(total/1024/1024/1024,3) | eval gauge_base=0 | eval gauge_danger=totalGB*0.8 | eval gauge_top=totalGB+0.001 | gauge usedGB gauge_base gauge_danger totalGB gauge_top',
    #'search index=network SCOPE ALARM',
    #'| rest splunk_server=local /services/licenser/slaves | mvexpand active_pool_ids | where warning_count>0 | eval pool=active_pool_ids | join type=outer pool [rest splunk_server=local /services/licenser/pools | eval pool=title | fields pool stack_id] | eval in_violation=if(warning_count>4 OR (warning_count>2 AND stack_id=="free"),"yes","no") | fields label, title, pool, warning_count, in_violation | fields - _timediff | rename label as "Slave" title as "GUID" pool as "Pool" warning_count as "Hard Warnings" in_violation AS "In Violation?"',
]

class Version:

    DIAG_2012 = "diag_2012"
    STORM_2013 = "storm_2013"
    DIAG_2014 = "diag_2014"


class DataSource(object):
    
    def __init__(self):
        pass

    def connect(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def get_interactive_queries(self):
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


class Database(DataSource):

    def __init__(self, wildcard):
        self.wildcard = wildcard
        self.connection = None
        super(Database, self).__init__()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None  

    def commit(self):
        if self.connection:
            self.connection.commit()

    def get_users(self):
        self.connect()
        users = {}
        cursor = self.execute("SELECT id, name, case_id, user_type FROM users")
        for (uid, name, case_id, user_type) in cursor.fetchall():
            logger.debug("[data] - Fetched user: " + name)
            user = User(name)
            user.case_id = case_id
            user.user_type = user_type
            yield user
        self.close()   

    def get_queries(self):
        self.connect()
        cursor = self.execute("SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
                        earliest_event, latest_event, range, is_realtime, \
                        splunk_search_id, execution_time, saved_search_name, \
                        user_id, session_id FROM queries")
        for (qid, text, time, interactive, suspicious, search_type, ev, lv, r, realtime, 
            splid, execution_time, spl_name, uid, sid) in cursor.fetchall():
            q = Query(text, time)
            q.query_id = qid
            q.user_id = uid
            q.session_id = sid
            q.search_type = search_type
            q.earliest_event = ev
            q.latest_event = lv
            q.range = r
            q.is_realtime = realtime
            q.splunk_search_id = splid
            q.execution_time = execution_time
            q.saved_search_name = spl_name
            q.is_interactive = interactive
            q.is_suspicious = suspicious
            yield q
        self.close()   

    def get_interactive_queries(self):
        self.connect()
        sqlquery = "SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id \
                    FROM queries \
                    WHERE is_interactive=%s" % self.wildcard
        cursor = self.execute(sqlquery, (True, ))
        for (qid, text, time, interactive, suspicious, search_type, ev, lv, r, realtime, 
            splid, execution_time, spl_name, uid, sid) in cursor.fetchall():
            q = Query(text, time)
            q.query_id = qid
            q.user_id = uid
            q.session_id = sid
            q.search_type = search_type
            q.earliest_event = ev
            q.latest_event = lv
            q.range = r
            q.is_realtime = realtime
            q.splunk_search_id = splid
            q.execution_time = execution_time
            q.saved_search_name = spl_name
            q.is_interactive = interactive
            q.is_suspicious = suspicious
            yield q
        self.close()   

    def get_parsetrees(self):
        self.connect()
        cursor = self.execute("SELECT parsetree, query_id FROM parsetrees")
        for (parsetree, query_id) in cursor.fetchall():            
            p = ParseTreeNode.loads(parsetree)
            p.query_id = query_id
            yield p
        self.close()   

    def get_sessions(self):
        self.connect()
        users = {}
        sessions = {}
        ucursor = self.execute("SELECT id, name, case_id, user_type FROM users")
        for (uid, name, case_id, user_type) in ucursor.fetchall():
            logger.debug("[data] - Fetched user: " + name)
            user = User(name)
            user.case_id = case_id
            user.user_type = user_type
            scursor = self.execute("SELECT id, user_id, session_type FROM sessions \
                                    WHERE user_id = " + self.wildcard, (uid,))
            for (sid, uid, session_type) in scursor.fetchall():
                sessions[sid] = Session(sid, user)
                sessions[sid].type = type 
                user.sessions[sid] = sessions[sid]
                qcursor = self.execute("SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
                                earliest_event, latest_event, range, is_realtime, \
                                splunk_search_id, execution_time, saved_search_name, \
                                user_id, session_id, parsetree FROM queries, parsetrees \
                                WHERE queries.id = parsetrees.query_id AND session_id = " + self.wildcard, (sid,))
                for (qid, text, time, auto, suspicious, search_type, ev, lv, r, realtime, 
                    splid, execution_time, spl_name, uid, sid, parsetree) in qcursor.fetchall():
                    session = sessions.get(sid, None)
                    q = Query(text, time)
                    q.query_id = qid
                    q.user_id = uid
                    q.user = user
                    q.session_id = sid
                    q.session = session
                    q.search_type = search_type
                    q.earliest_event = ev
                    q.latest_event = lv
                    q.range = r
                    q.is_realtime = realtime
                    q.splunk_search_id = splid
                    q.execution_time = execution_time
                    q.saved_search_name = spl_name
                    q.is_interactive = auto
                    q.is_suspicious = suspicious
                    q.parsetree = ParseTreeNode.loads(parsetree)
                    q.session = session
                    if session is not None:
                        session.queries.append(q)
                    user.queries.append(q)
        for session in sessions.itervalues():
            session.queries = sorted(session.queries, key=lambda x: x.time)
            if len(session.queries) > 1:
                yield session
        self.close()   
    
    def get_users_with_queries(self):
        self.connect()
        users = {}
        ucursor = self.execute("SELECT id, name, case_id, user_type FROM users LIMIT 20")
        for (uid, name, case_id, user_type) in ucursor.fetchall():
            logger.debug("[data] - Fetched user: " + name)
            user = User(name)
            user.case_id = case_id
            user.user_type = user_type
            #qcursor = self.execute("SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
            #                earliest_event, latest_event, range, is_realtime, \
            #                splunk_search_id, execution_time, saved_search_name, \
            #                user_id, session_id, parsetree FROM queries, parsetrees \
            #                WHERE queries.id = parsetrees.query_id AND user_id = " + self.wildcard, (uid,))
            qcursor = self.execute("SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id FROM queries \
                            WHERE user_id = " + self.wildcard, (uid,))
            for (qid, text, time, auto, suspicious, search_type, ev, lv, r, realtime, 
                splid, execution_time, spl_name, uid, sid) in qcursor.fetchall():
                #splid, execution_time, spl_name, uid, sid, parsetree) in qcursor.fetchall():
                q = Query(text, time)
                q.query_id = qid
                q.user_id = uid
                q.user = user
                q.session_id = sid
                q.search_type = search_type
                q.earliest_event = ev
                q.latest_event = lv
                q.range = r
                q.is_realtime = realtime
                q.splunk_search_id = splid
                q.execution_time = execution_time
                q.saved_search_name = spl_name
                q.is_interactive = auto
                q.is_suspicious = suspicious
                #q.parsetree = ParseTreeNode.loads(parsetree)
                user.queries.append(q)
            user.interactive_queries = [q for q in user.queries if q.is_interactive]
            user.noninteractive_queries = [q for q in user.queries if not q.is_interactive]
            yield user
        self.close()   
    

class PostgresDB(Database):

    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        super(PostgresDB, self).__init__("%s")

    def connect(self):
        self.connection = psycopg2.connect(database=self.database, user=self.user, password=self.password)
        return self.connection

    def execute(self, query, *params):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, *params)
        return cursor


class SQLite3DB(Database):
    
    def __init__(self, path):
        self.path = path
        super(SQLite3DB, self).__init__("?")

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        return self.connection

    def execute(self, query, *params):
        if not self.connection:
            self.connect()
        return self.connection.execute(query, *params)


class Files(DataSource):

    def __init__(self, path, module, version):
        self.path = path
        self.module = module
        self.version = version
        super(Files, self).__init__()

    def get_users(self):
        for user in self.get_users_with_queries():
            yield user

    def get_queries(self):
        count = 0
        for user in self.get_users_with_queries():
            for query in user.queries:
                query.query_id = count
                yield query
                count += 1

    def get_parsetrees(self):
        for query in self.get_queries():
            parsetree = parse_query(query)
            if parsetree:
                parsetree.query_id = query.query_id
                yield parsetree

    def get_sessions(self):
        texts = self.get_possibly_suspicious_texts()
        for user in self.get_users():
            self.remove_noninteractive_queries_by_search_type(user, version=self.version)
            user.suspicious = self.remove_suspicious_queries(user, texts)
            self.extract_sessions_from_user(user)
            for (num, session) in user.sessions.iteritems():
                yield session

    def get_users_with_queries(self):
        get_users = None
        if isfile(self.path):
            get_users = self.module.get_users_from_file
        if isdir(self.path):
            get_users = self.module.get_users_from_directory
        if get_users is None: # TODO: Raise error.
            print "Non-existent path:", self.path
            exit()
        for user in get_users(self.path):
            yield user

    def get_users_with_sessions(self):
        texts = self.get_possibly_suspicious_texts()
        for user in self.get_users():
            self.remove_noninteractive_queries_by_search_type(user, version=self.version)
            user.suspicious = self.remove_suspicious_queries(user, texts)
            self.extract_sessions_from_user(user)
            yield user

    def remove_noninteractive_queries_by_search_type(self, user, version=Version.DIAG_2014):
        if version == Version.DIAG_2014:
            handgenerated = "adhoc"
            user.noninteractive_queries = [query for query in user.queries if query.search_type != handgenerated]
            user.interactive_queries = [query for query in user.queries if query.search_type == handgenerated]
        elif version in [Version.DIAG_2012, Version.STORM_2013]:
            handgenerated = "historical"
            user.noninteractive_queries = [query for query in user.queries if query.search_type != handgenerated]
            user.interactive_queries = [query for query in user.queries if query.search_type == handgenerated]
        else:
            print "Unknown data version -- please provide a known version." # TODO: Raise error.
        for query in user.noninteractive_queries:
            query.is_interactive = False
        for query in user.interactive_queries:
            query.is_interactive = True

    def remove_suspicious_queries(self, user, texts):
        return True

    def extract_sessions_from_user(self, user):
        if len(user.interactive_queries) == 0:
            return
        session_id = 0
        user.interactive_queries.sort(key=lambda x: x.time)
        prev_time = curr_time = -1.
        session = Session(session_id, user)
        user.sessions[session_id] = session
        for query in user.interactive_queries:
            curr_time = query.time
            if prev_time < 0.:
                prev_time = curr_time
            query.delta = curr_time - prev_time
            if query.delta > NEW_SESSION_THRESH_SECS:
                self.update_session_duration(user.sessions[session_id])
                session_id += 1
                session = Session(session_id, user)
                user.sessions[session_id] = session
            prev_time = curr_time
            query.session = user.sessions[session_id]
            user.sessions[session_id].queries.append(query)
        self.update_session_duration(user.sessions[session_id])

    def update_session_duration(self, session):
        first_query = session.queries[0]
        last_query = session.queries[-1]
        session.duration = last_query.time - first_query.time


class JSONFiles(Files):
    
    def __init__(self, path, version):
        import jsondata
        super(JSONFiles, self).__init__(path, jsondata, version)


class CSVFiles(Files):
    
    def __init__(self, path, version):
        import csvdata
        super(CSVFiles, self).__init__(path, csvdata, version)
