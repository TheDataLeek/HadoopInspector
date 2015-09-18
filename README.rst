Hadoop\_Inspector
=================

For a more detailed analysis, please reference `the pdf
report <https://github.com/willzfarmer/HadoopInspector/blob/master/proposal/HadoopInspector.pdf>`__

Background
----------

Data quality problems have plagued analytical systems for twenty years:
continually appearing in the top four reasons for project failure.

In this space data quality problems loom large - a small defect that
could be safely ignored or forgotten in the transactional world hamper
queries and cause users to question our credibility for months.

The advent and innovation in Big Data and Data Science has not
diminished this challenge. On Hadoop specifically: \* Our data generally
lacks any enforced constraints to ensure data validity \* We are adding
data faster than ever, with less time to research upstream and ETL
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

Hadoop-Inspector is being built because we believe that the complexity
of a large, constantly loaded cluster defies an unmanaged approach or QA
testing in the development process. It requires something more like an
automobile assembly line: continuous quality control (QC) that can take
into account undocumented changes from upstream sources, accidental
changes to production, changes that bypass QA, etc. And it shouldn't be
limited to traditional quality tests, but should be able to test for
compliance with policies as well.

Architecture
------------

Current Status
--------------

Our initial focus has been on building a demo to help us validate ideas,
and build some of our UIs. This includes: - hadoopinspector-demogen.py -
which can generate 50,000+ check results against a hypothetical user
hadoop environment - hadoopinspector-runner.py - which runs checks from
repository and writes results - server - which runs a website that
allows the user to analyze these demo results - report - will produces a
pdf check result summary report

Packaged Checks - reusable tests
--------------------------------

Rolling Levenshtein Average with Standard Deviation Comparisons
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This test calculates the forward levenshtein values and then uses them
to calculate a rolling average. Each value as the column is iterated
over is compared to the rolling average and if the new value is too far
away (read: after a certain number of std. dev. away) it flags the
value.

This test uses a weighted moving average with the notion that data
inserted longer ago will have less "value" than upcoming data (as long
as the data is not rejected).

References: - https://en.wikipedia.org/wiki/Levenshtein\_distance -
https://en.wikipedia.org/wiki/Moving\_average#Weighted\_moving\_average

Installation
------------

-  ideally create a dedicated virtualenv
-  pip install hadoopinspector

Development
-----------

-  py.test within a virtualenv for python3.4 needs a little help - so
   run it with:

   -  $ python3 -m py.test

-  to simplify this with tox, create a packaged file of tests:

   -  $ python3 -m py.test --genscript=runtests.py
   -  $ chmod +x runtests.py
   -  $ cp runtests.py ../bin
   -  from there it's referenced by tox.ini, and will get run when you
      run tox
