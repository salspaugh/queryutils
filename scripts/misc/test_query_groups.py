from queryutils.databases import PostgresDB
l = "lupe"
p = PostgresDB(l, l, l)

#p.connect()
##cursor = p.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
##                earliest_event, latest_event, range, is_realtime, \
##                splunk_search_id, execution_time, saved_search_name, \
##                user_id, session_id \
##                FROM queries \
##                WHERE text=%s" % p.wildcard, (text,))
#queries = []
#for idx, query in enumerate(p.get_interactive_queries()):
#    if idx > 10: break
#    queries.append(query)
#
#for query in queries:
#    text = query.text
#    print text
#    cursor = p.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
#                    earliest_event, latest_event, range, is_realtime, \
#                    splunk_search_id, execution_time, saved_search_name, \
#                    user_id, session_id \
#                    FROM queries \
#                    WHERE is_interactive=%s AND text=%s" % (p.wildcard, p.wildcard), (True, text))
#    rows = cursor.fetchall()
#    print rows
#
#for q in p.get_interactive_queries_with_text(text):
#    print q

for query_group in p.get_query_groups():
    if query_group.query.text == "| metadata type=sourcetypes | search totalCount > 0":
        print query_group

