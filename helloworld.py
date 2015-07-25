#!/usr/bin/env python2.7

import sys
import sqlite3
import random
import Levenshtein
import threading
import Queue

def main():
    conn = sqlite3.connect(':memory:')
    testdb = TestDatabase(conn)
    testdb.insert_bad_row('classes')
    testdb.insert_bad_row('characters')
    test_levenshtein_distances(testdb, 'classes')
    test_levenshtein_distances(testdb, 'characters')

def test_levenshtein_distances(database, table):
    """
    This test calculates a weighted rolling levenshtein distance over the rows
    to determine statistical similarity.

    These Levenshtein instances are calculated forward.

    https://en.wikipedia.org/wiki/Levenshtein_distance
    https://en.wikipedia.org/wiki/Moving_average

    TODO: Add option for exponential WMA
    """
    results = Queue.Queue()
    database.cursor.execute('PRAGMA table_info({})'.format(table))
    tables = [t[1] for t in database.cursor.fetchall()]

    def column_levenshtein_rolling_average(raw_rows):
        rows = [t[0] for t in rows]
        distances = [Levenshtein.distance(rows[i], rows[i - 1]) for i in range(1, len(rows))]  # i'th is p_m
        total = 0
        for n in range(1, len(rows)):
            p = distances[i]  # i'th p == p_m
            total = 


    threads = []
    for tablename in tables:
        database.cursor.execute('SELECT {} FROM {}'.format(tablename, table))
        threads.append(threading.Thread(target=column_levenshtein_rolling_average,
                                        args=(database.cursor.fetchall(),)))
    [t.start() for t in threads]
    [t.join() for t in threads]


class TestDatabase(object):
    def __init__(self, connection):
        self.conn = connection
        self.cursor = self.conn.cursor()

        self._generate_tables()
        self._generate_data()

    def _generate_tables(self):
        self.cursor.execute(
            ('CREATE TABLE classes('
                'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
                'name STRING,'
                'desc STRING'
            ')'))
        self.cursor.execute(
            ('CREATE TABLE characters('
                'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
                'name STRING,'
                'class INTEGER,'
                'FOREIGN KEY(class) REFERENCES classes(id)'
            ')'))

    def _generate_data(self):
        self.cursor.executemany('INSERT INTO classes(name, desc) VALUES(?, ?)',
            [('Fighter', 'The default fighting class'),
                ('Cleric', 'A healing class'),
                ('Paladin', 'Fighting clerics'),
                ('Thief', 'One who steals from others'),
                ('Assassin', 'One who kills others for money'),
                ('Barbarian', 'A strong clumsy fighter'),
                ('Mage', 'A non fighting spellcaster')])
        self.cursor.executemany('INSERT INTO characters(name, class) VALUES(?, ?)',
                [('Bob the Mighty', 1),
                    ('Alfred the Butler', 4),
                    ('Batman', 5)])

    def insert_bad_row(self, table):
        if table == 'classes':
            self.cursor.execute('INSERT INTO classes(name, desc) VALUES(?, ?)',
                    (str(random.randint(10, 50000)), str(random.randint(0, 20))))
        elif table == 'characters':
            self.cursor.execute('INSERT INTO characters(name, class) VALUES(?, ?)',
                    (str(random.randint(10, 50000)), str(random.randint(97, 120))))




if __name__ == '__main__':
    sys.exit(main())
