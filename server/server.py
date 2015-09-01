#!/usr/bin/env python3

import sys, os
import json
import sqlite3
import numpy as np
import random
import datetime
from flask import Flask, render_template, Markup, request


app = Flask(__name__)

#TODO: Actually use config and determine configurable fields
config = json.loads(open('../config/config.json').read())

# TODO: Make javascript not explode when it runs out of colors
colors = [
    '#5DA5DA', # (blue)
    '#B276B2', # (purple)
    '#60BD68', # (green)
    '#F17CB0', # (pink)
    '#B2912F', # (brown)
    '#FAA43A', # (orange)
    '#4D4D4D', # (gray)
    '#DECF3F', # (yellow)
    '#F15854'] # (red)


#TODO: Replace dummy data code with real data from database

def main():
    app.run(host='localhost',
            port=config['port'],
            debug=True)


#TODO: Make this function more configurable
#TODO: Test this function!
def submit_query(query_string, args=None, flat=False):
    #TODO: Remove debug print statements
    print(query_string)
    print(args)
    connection = sqlite3.connect('../scripts/results.sqlite')
    cursor = connection.cursor()
    if args is None:
        cursor.execute(query_string)
    else:
        cursor.execute(query_string, args)
    results = list(cursor.fetchall())
    if flat:
        if results == []:
            return results
        else:
            return flatten(results)
    else:
        return results


#TODO: Test this
def flatten(l):
    return [item for sublist in l for item in sublist]


#TODO: Test this
def reformat_time(s):
    input_format = "%Y-%m-%d %H:%M:%S"
    output_format = "%Y-%m-%d"
    return datetime.datetime.strftime(datetime.datetime.strptime(s[:19],
                input_format), output_format)


@app.route('/', methods=['GET', 'POST'])
def root():
    names = submit_query('SELECT DISTINCT instance_name FROM check_results', flat=True)
    if request.method == 'POST':
        if request.form['searchquery'] == '':
            names = submit_query('SELECT DISTINCT instance_name FROM check_results', flat=True)
        else:
            names = submit_query(('SELECT DISTINCT(instance_name) '
                                            'FROM check_results '
                                            'WHERE instance_name LIKE ?'),
                            args=('%' + request.form['searchquery'] + '%',), flat=True)

    #TODO: Check that this string substitution is /ok/... Not sure if secure...
    #TODO: Use the real data dates
    history = []
    for name in names:
        instance_history = []
        i = 0
        for row in submit_query(('SELECT run_start_timestamp, check_violation_cnt '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'ORDER BY run_start_timestamp').format(name)):
            instance_history.append([reformat_time(row[0]), row[1]])
            i += 1
        history.append(instance_history)

    passing = [namehist[0][1] for namehist in history]

    numtests = np.random.randint(10, 10000, size=len(names))

    table_data = [[names[i], passing[i], numtests[i], passing[i]] for i in range(len(names))]

    content = render_template('instances.html',
                        colors=colors,
                        history=history,
                        table_data=table_data,
                        numnames=len(history),
                        numvals=[len(d) for d in history])
    return content


@app.route('/inspect/<instance>', methods=['GET', 'POST'])
def instance(instance):
    names = ['users', 'addresses', 'thisisaname']
    if request.method == 'POST':
        if request.form['searchquery'] == '':
            names = ['users', 'addresses', 'thisisaname']
        else:
            names = ['users']
    n = 30
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
    history = [[random.randint(0, 100) for i in range(n)] for j in range(len(names))]
    dates = [[datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=i)), '%Y-%m-%d')  for i in range(n)] for j in range(len(names))]

    data = []
    for i in range(len(names)):
        data.append([names[i], passing[i], numtests[i], 0 if passing[i] == 1 else random.randint(0, 5)])

    content = render_template('databases.html',
                                instance=instance,
                                colors=colors,
                                data=data,
                                dates=dates,
                                history=history,
                                history_length=len(history[0]),
                                numvals=len(history))
    return content


@app.route('/inspect/<instance>/<database>', methods=['GET', 'POST'])
def database(instance, database):
    names = ['table1', 'table2', 'table3', 'table4', 'table5']
    if request.method == 'POST':
        formkeys = list(request.form.keys())
        if 'savebutton' in formkeys:
            new_comment = request.form['commenttext']
        elif 'submitbutton' in formkeys:
            if request.form['searchquery'] == '':
                names = ['table1', 'table2', 'table3', 'table4', 'table5']
            else:
                names = ['table1', 'table2']
    n = 30
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
    dates = [[datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=i)), '%Y-%m-%d')  for i in range(n)] for j in range(len(names))]
    history = [[random.randint(0, 100) for i in range(n)] for j in range(len(names))]

    data = []
    for i in range(len(names)):
        data.append([names[i], passing[i], numtests[i], 0 if passing[i] == 1 else random.randint(0, 5)])

    content = render_template('tables.html',
                                database=database,
                                instance=instance,
                                colors=colors,
                                data=data,
                                history=history,
                                dates=dates,
                                history_length=len(history[0]),
                                numvals=len(history))
    return content


@app.route('/inspect/<instance>/<database>/<table>', methods=['GET', 'POST'])
def table(instance, database, table):
    names = list(['check{}'.format(i) for i in range(10)])
    if request.method == 'POST':
        if request.form['searchquery'] == '':
            names = list(['check{}'.format(i) for i in range(10)])
        else:
            names = list(['check{}'.format(i) for i in range(10)])[:2]
    n = 30
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
    history = [[random.randint(0, 100) for i in range(n)] for j in range(len(names))]
    dates = [[datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=i)), '%Y-%m-%d')  for i in range(n)] for j in range(len(names))]

    data = []
    for i in range(len(names)):
        data.append([names[i], passing[i], numtests[i], 0 if passing[i] == 1 else random.randint(0, 5)])

    content = render_template('checks.html',
                                instance=instance,
                                database=database,
                                table=table,
                                colors=colors,
                                data=data,
                                dates=dates,
                                history=history,
                                history_length=len(history[0]),
                                numvals=len(history))
    return content


@app.route('/inspect/<instance>/<database>/<table>/<check>', methods=['GET', 'POST'])
def checkdetails(instance, database, table, check):
    if request.method == 'POST':
        pass
    n = 30

    tables = ['table1', 'table2', 'table3', 'table4', 'table5']
    names = ['table1', 'table2', 'table3', 'table4', 'table5']

    desc = 'Proin suscipit luctus orci placerat fringilla. Donec hendrerit laoreet risus eget adipiscing. Suspendisse in urna ligula, a volutpat mauris. Sed enim mi, bibendum eu pulvinar vel, sodales vitae dui. Pellentesque sed sapien lorem, at lacinia urna. In hac habitasse platea dictumst. Vivamus vel justo in leo laoreet ullamcorper non vitae lorem. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin bibendum ullamcorper rutrum.'

    history = [random.randint(0, 100) for i in range(n)]
    dates = [datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=i)), '%Y-%m-%d')  for i in range(n)]

    content = render_template('checkdetails.html',
                        instance=instance,
                        database=database,
                        table=table,
                        check=check,
                        tables=tables,
                        desc=desc,
                        colors=colors,
                        history=history,
                        dates=dates,
                        numvals=1,
                        numtables=len(tables))
    return content


if __name__ == '__main__':
    sys.exit(main())
