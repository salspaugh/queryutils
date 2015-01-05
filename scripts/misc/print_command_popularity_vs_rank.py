import collections
import matplotlib.pyplot as plt
from queryutils.databases import PostgresDB
from queryutils.parse import tokenize_query

noncommands = [
    'PIPE',
    'LBRACKET',
    'RBRACKET',
    'MACRO',
    'ARGS',
    'EXTERNAL_COMMAND'
]

l = "lupe"
p = PostgresDB(l, l, l)

command_counts = collections.defaultdict(int)
p.connect()
print "Executing query."
cursor = p.execute("SELECT text FROM queries")
print "Query executed."
for row in cursor.fetchall():
    querystring = row["text"]
    tokens = tokenize_query(querystring)
    for token in tokens:
        if not token.type in noncommands:
            command_counts[token.value.strip().lower()] += 1
p.close()

command_counts_sorted = sorted(command_counts.iteritems(), key=lambda x: x[1], reverse=True)
print "{:42}{:5}".format("command", "count")
for (command, count) in command_counts_sorted:
    print "{:42}{:5}".format(command, count)

counts = [x[1] for x in command_counts_sorted]
ranks = range(len(counts))

plt.plot(ranks, counts)
plt.xlim(xmax=max(ranks))
plt.xlabel("Rank", fontsize=16)
plt.ylabel("Count", fontsize=16)
plt.savefig("command-counts-vs-rank.pdf")

            
