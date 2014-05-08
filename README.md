This project contains code for munging Splunk query logs.

This code extracts four main types of objects of interest:

1. [Queries](https://github.com/salspaugh/queryutils/blob/master/queryutils/query.py)
2. [Parsetrees](https://github.com/salspaugh/splparser/blob/master/splparser/parsetree.py)
3. [Users](https://github.com/salspaugh/queryutils/blob/master/queryutils/user.py)
4. [Sessions](https://github.com/salspaugh/queryutils/blob/master/queryutils/session.py)

These are used in other scripts that analyze Splunk queries,
for convenient parsing.


### Dependencies

See requirements.txt


### Installation

Currently, the best way to start using this code is to pull a version from
github:

`git clone https://github.com/salspaugh/queryutils.git`

Then add a pointer to it to your $PYTHONPATH:

`export PYTHONPATH=$PYTHONPATH:/path/to/queryutils`


### Getting started

1. Obtain Splunk query log data. The actual Splunk query log data is not 
provided with this project. For more information about how we obtained 
query log data and the formats we support, see the sections on
[data sources](#data-sources).

2. See `queryutils/datasource.py` for the available methods for 
extracting data from Splunk query logs.

3. *Recommended:* Import the query log data into a database. You can use
the scripts in `scripts` to do most of the work for you. For example, 
to set up a SQLite3 test database like the one already checked in, run:

```
./scripts/init_sqlite3.sh test/data/sqlite3.db
./scripts/load_base_sqlite3.sh
./scripts/load_parsed_sqlite3.sh
```

This creates a SQLite3 database in named sqlite3.db and load four tables
for your querying convenience. There are also scripts that demonstrate how
to load a Postgres database from the query logs.

4. *Optional:* Set up the test databases using the scripts in the scripts
directory and run the tests to make sure everything is working:

```python test/test_datasource.py```


### Data sources

This code is developed to work with queries from Splunk audit logs. To extract
queries from Splunk audit logs to analyze, you can run a search like the following:

```
search index="_internal" source=*audit.log (action=search) [search index="_internal" source=*systeminfo.txt splunk_version=6* | fields index]  | eval is_realtime=case(is_realtime == 0, "false", is_realtime == 1, "true", (is_realtime == "N/A" AND (search_id LIKE "rt%")), "true", (is_realtime == "N/A" AND NOT (search_id LIKE "rt%")), "false", 1==1, "false")  | rex "search_et=(?<search_et>.*?), search_lt" | rex "search_lt=(?<search_lt>.*?), is_realtime" | rex "search='(?<search>.*?)', autojoin" | eval search_id=replace(search_id,"'","") | eval search=replace(search,"'","") | eval search_type=case((id LIKE "DM_%" OR savedsearch_name LIKE "_ACCELERATE_DM%"), "dm_acceleration", search_id LIKE "scheduler%", "scheduled", search_id LIKE "subsearch%", "subsearch", (search_id LIKE "SummaryDirector%" OR search_id LIKE "summarize_SummaryDirector%"), "summary_director", 1=1, "adhoc") | eval user = if(user="n/a", null(), user) | stats min(_time) as _time first(user) as user max(total_run_time) as total_run_time first(search) as search values(savedsearch_name) AS savedsearch_name first(index) AS case_id first(search_type) AS search_type first(is_realtime) AS is_realtime, first(search_et) AS search_et last(search_lt) AS search_lt by search_id | search search_type!="subsearch" search_type!="summary_director" search_type!="dm_acceleration" search!=*_internal* search!=*_audit* | eval range=search_lt-search_et| fields case_id, search_type, is_realtime, search_id, user, search, savedsearch_name, search_et, search_lt, range, total_run_time | head 100000
```

This search was written to run on Splunk 6.0 but may work with some other versions.
You may have to modify this search depending on how you have set up your
Splunk deployment. You can find an example of data in this format in `test/data/diag2014.csv`.

*Known issues:* The above query labels some searches as "ad hoc" that appear to
in fact be generated programmatically, as opposed to hand-issued.

For older versions of Splunk, you may want to run:

```
search index="_internal" host=* action=search (id=* OR search_id=*) source="*audit.log" | eval search_id = if(isnull(search_id), id, search_id) | replace '*' with * in search_id | rex "search_et=(?<search_et>.*?), search_lt" | rex "search_lt=(?<search_lt>.*?), is_realtime" | rex "search='(?<search>.*?)', autojoin" | convert num(total_run_time) | eval user = if(user="n/a", null(), user) | stats values(search_et) as search_et values(search_lt) as search_lt min(_time) as _time first(user) as user max(total_run_time) as total_run_time first(search) as search by search_id | search search_id=* search=search* OR search=rtsearch* search!=*_internal* search!=*_audit* | eval range=(search_lt - search_et) | eval searchtype=case(like(search_id,"13%.%"),"historical",like(search_id,"rt_%"),"realtime",like(search_id,"scheduler__%"),"scheduled",like(search_id,"subsearch_%"),"subsearch",like(search_id,"remote_%"),"remote", like(search_id,"%search%"),"other") | search searchtype!=subsearch | fields + searchtype, search_lt, search_et, range, search_id, user, search | fields + searchtype, range, search_lt, search_et, search, search_id, user, error, savedsearch_name, total_run_time
```

You can find an example of data in this format in `test/data/diag2012.json`.
If you use this query to extract data, specify the version in the code as `diag2012`.
The default is `diag2014`.

*Known issues:* The search type labels for these queries are not trustworthy.

