Overview
========

queryutils is a set of Python modules for parsing and managing Splunk query data. Primarily, it's code to serialize Splunk query, user, and session data into and out of a database, as well as convert Splunk commands to and from a taxonomy that can be used to understand the tasks users perform using Splunk.

It includes facilities for parsing .csv and .json formatted Splunk query files 
and generating Query and User objects to store the information associated with each
query. It also includes code to serialize this data to a database (postgres or SQLite).

Please `submit an issue <https://github.com/salspaugh/queryutils/issues>`_ if you run into any problems.

Use `this link <mailto:saraalspaugh@gmail.com>`_ to send feedback.

Note: This documentation is a work-in-progress!
