#!/usr/bin/env python3

import sys, os
import json
import sqlite3
import numpy as np
import random
import datetime
from flask import Flask, render_template, Markup, request


app = Flask(__name__)

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


@app.route('/')
def root():
    n = 10
    names = ['dev', 'qa', 'prod']
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
    dates = [[datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=i)), '%Y-%m-%d')  for i in range(n)] for j in range(len(names))]
    history = [[random.randint(0, 100) for i in range(n)] for j in range(len(names))]

    data = []
    for i in range(len(names)):
        data.append([names[i], passing[i], numtests[i], 0 if passing[i] == 1 else random.randint(0, 5)])

    content = render_template('instances.html',
                        data=data,
                        colors=colors,
                        history=history,
                        dates=dates,
                        history_length=len(history[0]),
                        numvals=len(history))
    return content


@app.route('/inspect/<instance>')
def instance(instance):
    n = 30
    names = ['users', 'addresses', 'thisisaname']
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
    history = [[random.randint(0, 100) for i in range(n)] for j in range(len(names))]

    data = []
    for i in range(len(names)):
        data.append([names[i], passing[i], numtests[i], 0 if passing[i] == 1 else random.randint(0, 5)])

    content = render_template('databases.html',
                                instance=instance,
                                colors=colors,
                                data=data,
                                history=history,
                                history_length=len(history[0]),
                                numvals=len(history))
    return content


@app.route('/inspect/<instance>/<database>', methods=['GET', 'POST'])
def database(instance, database):
    if request.method == 'POST':
        new_comment = request.form['commenttext']
        # Insert comment into database
    n = 30
    names = ['table1', 'table2', 'table3', 'table4', 'table5']
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
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
                                history_length=len(history[0]),
                                numvals=len(history))
    return content


@app.route('/inspect/<instance>/<database>/<table>')
def table(instance, database, table):
    n = 30
    names = list(['check{}'.format(i) for i in range(10)])
    passing = np.random.randint(0, 1, size=len(names))
    numtests = np.random.randint(10, 10000, size=len(names))
    history = [[random.randint(0, 100) for i in range(n)] for j in range(len(names))]

    data = []
    for i in range(len(names)):
        data.append([names[i], passing[i], numtests[i], 0 if passing[i] == 1 else random.randint(0, 5)])

    content = render_template('checks.html',
                                instance=instance,
                                database=database,
                                table=table,
                                colors=colors,
                                data=data,
                                history=history,
                                history_length=len(history[0]),
                                numvals=len(history))
    return content


@app.route('/check/<check>')
def checkdetails(check):
    n = 30

    tables = ['table1', 'table2', 'table3', 'table4', 'table5']
    names = ['table1', 'table2', 'table3', 'table4', 'table5']

    desc = 'Proin suscipit luctus orci placerat fringilla. Donec hendrerit laoreet risus eget adipiscing. Suspendisse in urna ligula, a volutpat mauris. Sed enim mi, bibendum eu pulvinar vel, sodales vitae dui. Pellentesque sed sapien lorem, at lacinia urna. In hac habitasse platea dictumst. Vivamus vel justo in leo laoreet ullamcorper non vitae lorem. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin bibendum ullamcorper rutrum.'

    history = [random.randint(0, 100) for i in range(n)]
    dates = [datetime.datetime.strftime((datetime.datetime.now() - datetime.timedelta(days=i)), '%Y-%m-%d')  for i in range(n)]

    content = render_template('checkdetails.html',
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
