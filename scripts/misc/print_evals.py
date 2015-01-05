
from queryutils.databases import PostgresDB
from queryutils.parse import tokenize_query

def print_eval_portions(query):
    eval = []
    push = False
    for token in tokenize_query(query):
        if token.type == "PIPE":
            if len(eval) > 0:
                print " ".join(eval)
                eval = []
            push = False
        if push:
            eval.append(token.value)
            continue
        if token.type == "EVAL":
            push = True
            eval.append(token.value)
            continue

l = "lupe"
p = PostgresDB(l, l, l)

p.connect()
cursor = p.execute("SELECT distinct text FROM queries")
for row in cursor.fetchall():
    querystring = row["text"]
    if querystring.find("eval") < 0:
        continue
    print_eval_portions(querystring)
p.close()
            
