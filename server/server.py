#!/usr/bin/env python3

import sys
import json
import random
import sqlite3
import csv
from flask import Flask, render_template, Markup
import matplotlib.pyplot as plt
import mpld3

app = Flask(__name__)
config = json.loads(open('../config/config.json').read())

print(config)

connection = sqlite3.connect(config["db"])
cursor = connection.cursor()

cols = {'instance':'STRING',
        'database_name':'STRING',
        'table_name':'STRING',
        'table_partitioned':'INTEGER',
        'run_start_timestamp':'STRING',
        'run_mode':'STRING',
        'partition_key':'STRING',
        'partition_value':'STRING',
        'check_name':'STRING',
        'check_policy_type':'STRING',
        'check_type':'STRING',
        'run_check_start_timestamp':'STRING',
        'run_check_end_timestamp':'STRING',
        'run_check_mode':'STRING',
        'run_check_rc':'INTEGER',
        'run_check_violation_cnt':'INTEGER',
        'run_check_anomaly_score':'INTEGER',
        'run_check_scope':'INTEGER',
        'run_check_unit':'STRING',
        'run_check_severity_score':'INTEGER',
        'run_check_validated':'STRING'}

sql_cols = ', '.join(['{} {}'.format(key, item) for key, item in cols.items()])

cursor.execute('CREATE TABLE records({})'.format(sql_cols))

with open('/tmp/inspector_demo.csv') as f:
    dr = csv.DictReader(f)
    to_db = [tuple(i[k] for k in cols.keys()) for i in dr]

print(to_db)


sys.exit(0)

@app.route('/')
def root():
    # TODO: REPLACE CHUNK
    dbs, tables = gen_tables()
    rules = [get_number() for _ in dbs]
    checks = [get_number() for _ in dbs]
    rimages = [gen_image() for _ in rules]
    cimages = [gen_image() for _ in checks]
    table = [[dbs[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(dbs))]
    ###

    content = render_template('index.html',
                              name='Hadoop QA',
                              table=table)
    return content


@app.route('/<database>')
def database(database):
    # TODO: Replace CHUNK
    dbs, tables = gen_tables()
    rules = [get_number() for _ in tables]
    checks = [get_number() for _ in tables]
    rimages = [gen_image() for _ in rules]
    cimages = [gen_image() for _ in checks]
    table = [[tables[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(tables))]
    ###

    content = render_template('database.html',
                              name=database,
                              table=table)
    return content


@app.route('/<database>/<table>')
def table(database, table):
    return str("TODO")


# TODO: Replace
def get_number():
    return random.randint(0, 1000)


# TODO: REPLACE
def get_numbers(n):
    return [random.randint(0, 1000) for i in range(n)]


# TODO: Replace
def gen_tables():
    dbs = ['security', 'metrics']
    tables = ['users', 'metrics', 'addresses']
    return dbs, tables


def gen_image():
    fig = plt.figure(figsize=(5, 1))
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    ax.plot(get_numbers(100))
    return Markup(mpld3.fig_to_html(fig))


if __name__ == '__main__':
    app.run(host='localhost',
            port=config['port'],
            debug=True)
