Hadoop\_Inspector
=================

To review our complete set of documentation please see our `wiki
page <https://github.com/willzfarmer/HadoopInspector/wiki>`__

Background
----------

Data quality problems have plagued analytical systems for twenty years:
continually appearing in the top four reasons for project failure.

In this space data quality problems loom large - a small defect that
could be safely ignored or forgotten in the transactional world hamper
queries and cause users to question our credibility for months.

The advent and innovation in Big Data and Data Science has not
diminished this challenge. On Hadoop specifically: \* Data generally
lacks any enforced constraints to ensure data validity \* Data is being
added faster than ever, with less time to research upstream and ETL
pipeline issues \* We are building vast systems, sometimes with hundreds
of thousands of tasks being defined \* We often have democratized access
to our clusters - with dozens of different people adding data.

Additionally, in these large clusters most teams struggle to comply with
policies and other requirements, whether regulatory, corporate or
defined by their own teams. These might define general data retention
requirements, or specific requirements for individual tables. They might
define table naming conventions, security requirements, or stats aging &
collection requirements.

Objective
---------

Hadoop-Inspector was built to address the needs to manage data quality
within large, complex, and constantly loaded clusters that were
unfulfilled by simple QA testing during development. It offers a
solution more like an automobile assembly line: continuous quality
control (QC) that can account for changes to upstream systems,
accidental changes to production, data migration errors, and ETL/Ingest
defects.

Current Status
--------------

Our current focus has been to complete a minimum viable product that
includes:

-  hadoopinspector-demogen.py - which can generate 50,000+ check results
   against a hypothetical user hadoop environment. This is used to
   excercize the UI.
-  hadoopinspector-runner.py - a test-runner that writes results to a
   SQLite database.
-  hapinsp\_httpserver.py - serves the UI.

More info is on the
`wiki <https://github.com/willzfarmer/HadoopInspector/wiki>`__

Installation
------------

-  ideally create a dedicated virtualenv
-  pip install hadoopinspector
-  requires python 2.7

Licensing
=========

This source code is protected by the BSD license. See the file "LICENSE"
in the source code root directory for the full language or refer to it
here: http://opensource.org/licenses/BSD-3-Clause Copyright 2015 Will
Farmer and Ken Farmer
