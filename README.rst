Impala Winning Project
======================

Tests
-----

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
