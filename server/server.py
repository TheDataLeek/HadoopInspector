#!/usr/bin/env python3

import sys
import json
import datetime
import random
import sqlite3
import csv
from flask import Flask, render_template, Markup
import matplotlib.pyplot as plt
import mpld3


app = Flask(__name__)
config = json.loads(open('../config/config.json').read())

def main():
    setupdb()
    app.run(host='localhost',
            port=config['port'],
            debug=True)


def setupdb():
    connection = sqlite3.connect(config["db"])
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS records')
    cols = {'instance_name':'STRING',
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
    keys = cols.keys()    # Sets the order
    sql_cols = ', '.join(['{} {}'.format(key, cols[key]) for key in keys])
    cursor.execute('CREATE TABLE records({})'.format(sql_cols))
    with open('/tmp/inspector_demo.csv') as f:
        dr = csv.DictReader(f)
        to_db = [tuple(i[k] for k in keys) for i in dr]
    query = 'INSERT INTO records({}) VALUES ({});'.format(
                    ','.join(keys), ','.join(['?' for _ in keys]))
    cursor.executemany(query, to_db)
    connection.commit()
    connection.close()

def strtotime(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

def query(q, args=None):
    connection = sqlite3.connect(config["db"])
    cursor = connection.cursor()
    if args:
        cursor.execute(q, args)
    else:
        cursor.execute(q)
    res = cursor.fetchall()
    return res

#TODO: Clean up aggregation code and bin
@app.route('/')
def root():
    instances = [t[0] for t in query('SELECT DISTINCT instance_name FROM records')]

    # TODO: clean up this MAGIC
    rules = [query('SELECT SUM(run_check_violation_cnt) FROM records WHERE instance_name=?', (instance,))[0][0] for instance in instances]
    rule_history = [[t[0] for t in sorted(p, key=lambda t: strtotime(t[1]))] for p in
                        [query('SELECT run_check_violation_cnt, run_check_end_timestamp FROM records WHERE instance_name=?', (instance,)) for instance in instances]]
    rimages = [gen_image(history) for history in rule_history]
    checks = [query('SELECT SUM(run_check_anomaly_score) FROM records WHERE instance_name=?', (instance,))[0][0] for instance in instances]
    check_history = [[t[0] for t in sorted(p, key=lambda t: strtotime(t[1]))] for p in
                        [query('SELECT run_check_anomaly_score, run_check_end_timestamp FROM records WHERE instance_name=?', (instance,)) for instance in instances]]
    cimages = [gen_image(history) for history in check_history]

    table = [[instances[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(instances))]

    content = render_template('index.html', name='Hadoop QA', table=table)
    return content

@app.route('/inspect/<instance>')
def instance(instance):
    dbs = [t[0] for t in query('SELECT DISTINCT database_name FROM records WHERE instance_name=?', (instance,))]

    # TODO: clean up this MAGIC
    rules = [query('SELECT SUM(run_check_violation_cnt) FROM records WHERE instance_name=? AND database_name=?', (instance, database))[0][0] for database in dbs]
    rule_history = [[t[0] for t in sorted(p, key=lambda t: strtotime(t[1]))] for p in
                        [query('SELECT run_check_violation_cnt, run_check_end_timestamp FROM records WHERE instance_name=? AND database_name=?', (instance, database)) for database in dbs]]
    rimages = [gen_image(history) for history in rule_history]
    checks = [query('SELECT SUM(run_check_anomaly_score) FROM records WHERE instance_name=? AND database_name=?', (instance, database))[0][0] for database in dbs]
    check_history = [[t[0] for t in sorted(p, key=lambda t: strtotime(t[1]))] for p in
                        [query('SELECT run_check_anomaly_score, run_check_end_timestamp FROM records WHERE instance_name=? AND database_name=?', (instance, database)) for database in dbs]]
    cimages = [gen_image(history) for history in check_history]

    table = [[dbs[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(dbs))]

    content = render_template('tabular.html',
                              name=instance,
                              table=table)
    return content


@app.route('/inspect/<instance>/<database>')
def database(instance, database):
    tables = [t[0] for t in query('SELECT DISTINCT table_name FROM records WHERE database_name=? AND instance_name=?', (database, instance))]

    # TODO: clean up this MAGIC
    rules = [query('SELECT SUM(run_check_violation_cnt) FROM records WHERE instance_name=? AND database_name=? AND table_name=?', (instance, database, table))[0][0] for table in tables]
    rule_history = [[t[0] for t in sorted(p, key=lambda t: strtotime(t[1]))] for p in
                        [query('SELECT run_check_violation_cnt, run_check_end_timestamp FROM records WHERE instance_name=? AND database_name=? AND table_name=?', (instance, database, table)) for table in tables]]
    rimages = [gen_image(history) for history in rule_history]
    checks = [query('SELECT SUM(run_check_anomaly_score) FROM records WHERE instance_name=? AND database_name=? AND table_name=?', (instance, database, table))[0][0] for table in tables]
    check_history = [[t[0] for t in sorted(p, key=lambda t: strtotime(t[1]))] for p in
                        [query('SELECT run_check_anomaly_score, run_check_end_timestamp FROM records WHERE instance_name=? AND database_name=? AND table_name=?', (instance, database, table)) for table in tables]]
    cimages = [gen_image(history) for history in check_history]

    table = [[tables[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(tables))]

    content = render_template('tabular.html',
                              name=database,
                              table=table)
    return content


@app.route('/inspect/<instance>/<database>/<table>')
def table(instance, database, table):
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


def gen_image(points):
    # Assumes equal point spacing
    fig = plt.figure(figsize=(5, 1))
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    ax.plot(points)
    return Markup(mpld3.fig_to_html(fig))


if __name__ == '__main__':
    sys.exit(main())
