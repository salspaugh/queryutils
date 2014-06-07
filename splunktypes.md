### Notation

- results: a list *R* of events
- event: a list *E* of fields containing at least those fields required by Splunk (e.g., _time)
- field: a key-value pair *f* with a field name as the key i.e. a (field name, value) tuple. The type of a field is the set of types its values can take on.
- field name: a string *k* that is a valid identifier in Splunk
- value: the content *v* of a field (`#TODO(salspaugh): list valid types for values`)

### Multi-value operations

*Combine events that match everywhere except for one field into one event where the values of non-matching field have been transformed into a multi-value type consisting of a list of all values from the combined events (remove events, transform field).*

- mvcombine
    
    - Input: *R* = [*E1*, ..., *En*], *f*
    - Output: *R'* = [*E1*, ..., *Em*]
    - Such that: *m* <= *n*, and some subset of events *E* in *R* such that all values in all fields aside from *f* match, has been replaced with *E'*, etc. 

*Expand matching event on one field (add events, transform field).*

- mvexpand

### Filters

*Filter events based on user input.* 

-  delete
-  regex
-  search
-  multisearch
-  where

*Filter events based on other events.*

-  dedup
-  uniq

*Filter new events based on other events after generating new events by any means.*

- set

*Filter events based on index.*

- head
- tail

*Filter events based on metadata.*

- input

*Filter fields based on user input, possibly formatted as a table.*

- fields
- table

### Augment (add fields)

*Additional field(s) in each event that is function of other field(s) in prior events.*

- accum
- autoregress
- concurrency
- delta
- streamstats
- trendline

f( { r_1 : (c_1,...,c_k), ..., r_s : (c_1,..,c_k) } ) 
    = { r_1 : (c_1,...,c_k, c_k+1 = g(r_1:c_i)), ..., r_s : (c_1,..,c_k, c_k+1 = g(r_1:c_i,...r_s:c_i)) }

*Additional field in each event that is function of other field(s) in all events and possibly user input.*

- erex
- eventstats
- predict

*Additional field in each event that is function of other field in all events, aggregated by other field, filtered by value of additional field.*

- rare
- sirare
- top
- sitop

*Additional field in each event that is function of (_raw) field in all events and user input of previous command.*

- relevancy * depends on previous command

*Additional field(s) in each event that is function of all fields in all events.*

- cluster
- kmeans (also reorders)
 
*Additional fields in each event that is a global function of metadata.*

- addinfo

f( { r_1 : (c_1,...,c_k), ..., r_s : (c_1,..,c_k) } ) 
    = { r_1 : (c_1,...,c_k,g_1(),...,g_n()), ..., r_s : (c_1,..,c_k,g_1(),...g_n()) }

*Additional field(s) in each event that is function of other field(s) in same event.* 

-  addtotals
-  extract (kv)
-  kvform
-  outputtext *this is a confusing name since it doesn't do what outputcsv does
-  rangemap
-  reltime
-  rex
-  strcat
-  tag
-  typer
-  xmlkv
-  xpath

*Additional field in each event that is function of same or other field(s) in same event and user input.*

- eval
- spath

*Additional fields in each event that is function of fields of two events, pairwise.*

- join 
- selfjoin

*Additional field in each event that is function of a subset of previous events, optionally after an aggregation.*

- anomalies

f( { r_i : (c_1,...,c_k), ..., r_j : (c_1,..,c_k) } ) 
    = { r_i : (c_1,...,c_k, c_k+1 = g(r_i)), ..., r_j : (c_1,..,c_k, c_k+1 = g(r_i,...r_j-1)) }

*Additional field(s) in each event that are any function of anything.*

- appendcols

### Input (add events)

*Additional event with fields that is function of same field in all events.* 

- addcoltotals

f( { r_1 : (c_1,...,c_k), ..., r_s : (c_1,..,c_k) } ) 
    = { r_k+1 : (g(r_1:c_1,...,r_s:c_1),..., g(r_1:c_k,...,r_s:c_k) }

*Additional event with field that is function of same field in two other events.*

- diff

*Additional events with possibly same or possibly different fields that is any function of anything.*

- append
- appendpipe

*Additional events that with fields that are a function of single event.*

- multikv

### Transform

*Transform entries based on function of same entry and optionally user input.*

- convert (g(x) = int(x)
- fieldformat
- fillnull (g(x) = not null)
- nomv
- makemv
- replace (g(x) = y if x == k, otherwise x)
- scrub (g(x) = anonymized x)
- setfields (g(x) = c)
- xmlunescape (g(x) = unescaped x)

f( r_data, c_data ) = v 

*Transform entries based on function of other entries in the same field in all events.*

- bucket
- bucketdir
- outlier

*Transform entries based on function of other entries in the same field in prior events.*

- filldown

### Output and meta-commands

*Outputs data.*

- collect
- outputcsv
- outputlookup
- sendemail

*Controls where computation occurs.*

- localop

*Calls external command.*

- run
- script

### Reorder

- reverse
- rtorder
- sort

### Aggregate

*Single result that is function of all other events.*

- eventcount (count(*))
- single result that is function of all other events and user input
- format

*Different events and fields that are function of external data, optionally formatted as a table.* 

- crawl
- inputlookup 
- inputcsv
- loadjob
- lookup
- rest
- savedsearch

f( { r_1 : (c_1,...,c_k), ..., r_s : (c_1,..,c_k) } ) 
 = { u_1 : (d_1,...,d_j), ..., u_t : (d_1,...,d_j) }

*Different events and fields that are function of metadata, optionally formatted as a table.* 

- audit
- dbinspect
- history
- metadata
- metasearch
- typeahead

f( { r_1 : (c_1,...,c_k), ..., r_s : (c_1,..,c_k) } ) 
 = { u_1 : (d_1,...,d_j), ..., u_t : (d_1,...,d_j) }

*Different events and fields that are function of all events and certain fields.*

- sistats
- stats
- transpose

*Different events and fields that are function of a filter of all events and all fields.*
- return

*Group events.*
- transaction


### Visual-effects

*Different events and fields that are complicated function of all events and some fields, formatted as a table.*

- associate
- analyzefields
- contingency
- correlate
- fieldsummary

*Transform entries based on function of same entry.*

- iconify

*Transform entries based on function of same entry and user input.*

- highlight

*Transform or add fields based on function of same or other fields in the same event and user input.*

- chart
- timechart
- sichart
- sitimechart

### Rename

- rename

### Uncategorized

- abstract (filter of some sort)
- anomalousvalue
- findtypes
- folderize
- gauge
- gentimes
- iplocation
- localize
- makecontinuous (additional events to make field values continuous?)
- map
- overlap
- searchtxn
- typelearner
- untable
- xyseries (new events and fields of some sort)

