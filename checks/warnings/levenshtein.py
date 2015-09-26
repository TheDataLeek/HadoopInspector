#!/usr/bin/env python2.7

import sys
import random
import Levenshtein

def main():
    rows = [[random.randint(0, 10) if (j % 2) == 0 else 'test string' for i in range(10)] for j in range(1000)]
    cma = levenshtein_distances(rows)
    print(cma)

def levenshtein_distances(rows):
    """
    This test calculates a weighted rolling levenshtein distance over the rows
    to determine statistical similarity.

    These Levenshtein instances are calculated forward.

    https://en.wikipedia.org/wiki/Levenshtein_distance
    https://en.wikipedia.org/wiki/Moving_average

    TODO: Add option for WMA
    """
    cma = [v for v in rows[0]]
    n = 1
    for i in range(1, len(rows)):
        for j in range(len(rows[0])):
            value = Levenshtein.distance(str(rows[i][j]), str(rows[i - 1][j]))
            cma[j] = (value + (n * cma[j])) / (n + 1)
        n += 1
    return cma






if __name__ == '__main__':
    sys.exit(main())
