#!/usr/bin/env python

from queryutils import get_user_sessions
from queryutils import BYTES_IN_MB

def main():

    stats_dict = {}
    stats_dict['total_users'] = 0.
    stats_dict['total_sessions'] = 0.
    stats_dict['total_queries'] = 0.
    stats_dict['total_autorecurring'] = 0.
    
    stats_dict['queries_per_user'] = 0.
    stats_dict['queries_per_session'] = 0.
    stats_dict['sessions_per_user'] = 0.
    stats_dict['autorecurring_per_user'] = 0.
    
    stats_dict['sum_repeat_delta'] = 0.
    stats_dict['average_repeat_delta'] = 0.

    stats_dict['earliest_query'] = 1e15
    stats_dict['latest_query'] = 0.
    stats_dict['time_span'] = 0. 
    
    stats_dict['unique_queries'] = {}
    stats_dict['total_unique_queries'] = 0.
     
    iter = 0
    for users in  get_user_sessions(limit=800*BYTES_IN_MB):
        update_basic_stats(stats_dict, users)
        print "Processed " + str(iter) + "-th batch of users."
        iter += 1

    print_basic_stats(stats_dict)
    
    #users = get_user_sessions()
    #print_basic_stats(users)

def update_basic_stats(stats_dict, users):
    
    stats_dict['total_users'] += len(users)
    
    queries = []
    
    for user in users:

        stats_dict['total_sessions'] += len(user.sessions.keys())
        stats_dict['total_queries'] += len(user.queries)
        stats_dict['total_autorecurring'] += len(user.autorecurring_queries)

        for query in user.autorecurring_queries:
            stats_dict['sum_repeat_delta'] += query.repeat_delta

        queries = queries + [q.text for q in user.queries] 
        user.queries.sort(key=lambda x: x.time)
        if len(user.queries) > 0:
            first_query = user.queries[0].time
            last_query = user.queries[len(user.queries)-1].time
            if first_query < stats_dict['earliest_query']:
                stats_dict['earliest_query'] = first_query
            if last_query > stats_dict['latest_query']:
                stats_dict['latest_query'] = last_query
    
        for query in queries:
            if not query in stats_dict['unique_queries']:
                stats_dict['unique_queries'][query] = 0.
            stats_dict['unique_queries'][query] += 1.
        
    stats_dict['queries_per_user'] = stats_dict['total_queries'] / stats_dict['total_users']
    stats_dict['queries_per_session'] = stats_dict['total_queries'] / stats_dict['total_sessions']
    stats_dict['sessions_per_user'] = stats_dict['total_sessions'] / stats_dict['total_users']
    stats_dict['autorecurring_per_user'] = stats_dict['total_autorecurring'] / stats_dict['total_users']
    stats_dict['average_repeat_delta'] = stats_dict['sum_repeat_delta'] / stats_dict['total_autorecurring']
    
    stats_dict['total_unique_queries'] = len(stats_dict['unique_queries'].keys())
    
    stats_dict['time_span'] = stats_dict['latest_query'] - stats_dict['earliest_query']

def print_basic_stats(stats_dict):
    
    print "Total users: " + str(stats_dict['total_users'])
    print "Total sessions: " + str(stats_dict['total_sessions'])
    print "Total queries: " + str(stats_dict['total_queries'])
    print "Total autorecurring: " + str(stats_dict['total_autorecurring'])
    print "Average repeat delta (seconds): " + str(stats_dict['average_repeat_delta'])
    print "Total unique queries: " + str(stats_dict['total_unique_queries'])
    print "Queries per session: " + str(stats_dict['queries_per_session'])
    print "Queries per user: " + str(stats_dict['queries_per_user'])
    print "Sessions per user: " + str(stats_dict['sessions_per_user'])
    print "Autorecurring per user: " + str(stats_dict['autorecurring_per_user'])
    print "Time spanned (seconds): " + str(stats_dict['time_span'])

def print_basic_stats_from_users(user_sessions):

    total_users = float(len(user_sessions))
    total_sessions = 0.
    total_queries = 0.
    queries_per_session = 0.
    earliest_query = 9e15
    latest_query = -1.
    queries = []

    for user in user_sessions:
        total_sessions += len(user.sessions.keys())
        total_queries += len(user.queries)
        queries = queries + [q.text for q in user.queries] 
        user.queries.sort(key=lambda x: x.time)
        first_query = user.queries[0].time
        last_query = user.queries[len(user.queries)-1].time
        if first_query < earliest_query:
            earliest_query = first_query
        if last_query > latest_query:
            latest_query = last_query

    queries_per_user = total_queries / total_users
    queries_per_session = total_queries / total_sessions
    sessions_per_user = total_sessions / total_users
    time_span = latest_query - earliest_query

    unique_queries = {}
    for query in queries:
        if not query in unique_queries:
            unique_queries[query] = 0.
        unique_queries[query] += 1.
    total_unique_queries = len(unique_queries.keys())

    print "Total users: " + str(total_users)
    print "Total sessions: " + str(total_sessions)
    print "Total queries: " + str(total_queries)
    print "Total unique queries: " + str(total_unique_queries)
    print "Queries per session: " + str(queries_per_session)
    print "Queries per user: " + str(queries_per_user)
    print "Sessions per user: " + str(sessions_per_user)
    print "Time spanned (seconds): " + str(time_span)

# TODO: Session-length distribution.
# TODO: Most popular queries or query templates.
# TODO: Query interarrival per user to justify session threshold definition.

if __name__ == "__main__":
    main()
