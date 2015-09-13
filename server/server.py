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

# TODO: Make javascript not explode when it runs out of colors (more cleanly....)
colors = [
    '#5DA5DA', # (blue)
    '#B276B2', # (purple)
    '#60BD68', # (green)
    '#F15854', # (red)
    '#F17CB0', # (pink)
    '#B2912F', # (brown)
    '#FAA43A', # (orange)
    '#DECF3F', # (yellow)
    '#5DA5DA', # (blue)
    '#B276B2', # (purple)
    '#60BD68', # (green)
    '#F15854', # (red)
    '#F17CB0', # (pink)
    '#B2912F', # (brown)
    '#FAA43A', # (orange)
    '#DECF3F', # (yellow)
    '#5DA5DA', # (blue)
    '#B276B2', # (purple)
    '#60BD68', # (green)
    '#F15854', # (red)
    '#F17CB0', # (pink)
    '#B2912F', # (brown)
    '#FAA43A', # (orange)
    '#DECF3F', # (yellow)
    '#4D4D4D' # (gray)
    ]

#TODO: Replace dummy data code with real data from database

def main():
    app.run(host='localhost',
            port=config['port'],
            debug=True)


#TODO: Make this function more configurable
#TODO: Test this function!
def submit_query(query_string, args=None, flat=False):
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
def reformat_time(s, input_format='%Y-%m-%d %H:%M:%S', output_format='%Y-%m-%d'):
    return datetime.datetime.strftime(datetime.datetime.strptime(s, input_format), output_format)


@app.route('/', methods=['GET', 'POST'])
def root():
    # First get all instane names
    get_all_names = lambda : submit_query('SELECT DISTINCT instance_name FROM check_results', flat=True)
    names = get_all_names()
    # Apply search if applicable.
    if request.method == 'POST':
        # If empty search, reset to full
        if request.form['searchquery'] == '':
            names = get_all_names()
        else:
            names = submit_query(('SELECT DISTINCT(instance_name) '
                                            'FROM check_results '
                                            'WHERE instance_name LIKE ?'),
                            args=('%' + request.form['searchquery'] + '%',), flat=True)

    #TODO: Check that this string substitution is /ok/... Not sure if secure...
    # Now get history in nested dict.
    history  = {}
    metadata = {}
    for name in names:
        history[name] = {}
        metadata[name] = {}
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'GROUP BY yr_mon '
                                            'ORDER BY yr_mon').format(name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m"), row[1]])
        history[name]['year'] = instance_history
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'GROUP BY yr_mon_day '
                                            'ORDER BY yr_mon_day').format(name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history[name]['month']   = instance_history[-32:]
        history[name]['week']    = instance_history[-8:]
        metadata[name]['passing'] = submit_query(('SELECT run_start_timestamp, check_violation_cnt '
                                                    'FROM check_results '
                                                    'WHERE instance_name="{}" '
                                                    'ORDER BY run_start_timestamp DESC '
                                                    'LIMIT 1').format(name))[0][1]
        metadata[name]['yearlen'] = len(history[name]['year'])
        metadata[name]['monthlen'] = len(history[name]['month'])
        metadata[name]['weeklen'] = len(history[name]['week'])

    content = render_template('instances.html',
                        colors=colors,
                        names=names,
                        history=history,
                        metadata=metadata)
    return content


@app.route('/inspect/<instance>', methods=['GET', 'POST'])
def instance(instance):
    get_all_names = lambda : submit_query(('SELECT DISTINCT(database_name) '
                                            'FROM check_results '
                                            'WHERE instance_name=?'),
                                            args=(instance,), flat=True)
    names = get_all_names()
    if request.method == 'POST':
        if request.form['searchquery'] == '':
            names = get_all_names()
        else:
            names = submit_query(('SELECT DISTINCT(database_name) '
                                            'FROM check_results '
                                            'WHERE instance_name=? '
                                            'AND database_name LIKE ?'),
                            args=('%' + request.form['searchquery'] + '%',), flat=True)

    history  = {}
    metadata = {}
    for name in names:
        history[name] = {}
        metadata[name] = {}
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'AND database_name="{}" '
                                            'GROUP BY yr_mon '
                                            'ORDER BY yr_mon').format(instance, name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m"), row[1]])
        history[name]['year'] = instance_history
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'AND database_name="{}" '
                                            'GROUP BY yr_mon_day '
                                            'ORDER BY yr_mon_day').format(instance, name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history[name]['month']   = instance_history[-32:]
        history[name]['week']    = instance_history[-8:]
        metadata[name]['passing'] = submit_query(('SELECT run_start_timestamp, check_violation_cnt '
                                                    'FROM check_results '
                                                    'WHERE instance_name="{}" '
                                                    'AND database_name="{}" '
                                                    'ORDER BY run_start_timestamp DESC '
                                                    'LIMIT 1').format(instance, name))[0][1]
        metadata[name]['yearlen'] = len(history[name]['year'])
        metadata[name]['monthlen'] = len(history[name]['month'])
        metadata[name]['weeklen'] = len(history[name]['week'])

    content = render_template('databases.html',
                                instance=instance,
                                colors=colors,
                                names=names,
                                history=history,
                                metadata=metadata)
    return content


@app.route('/inspect/<instance>/<database>', methods=['GET', 'POST'])
def database(instance, database):
    get_all_names = lambda : submit_query(('SELECT DISTINCT table_name '
                                            'FROM check_results '
                                            'WHERE instance_name=? '
                                            'AND database_name=?'),
                                    args=(instance, database), flat=True)
    names = get_all_names()
    if request.method == 'POST':
        if request.form['searchquery'] == '':
            names = get_all_names()
        else:
            names = submit_query(('SELECT DISTINCT table_name '
                                        'FROM check_results '
                                        'WHERE instance_name=? '
                                        'AND database_name=? '
                                        'AND table_name LIKE ?'),
                                args=(instance, database,
                                        '%' + request.form['searchquery'] + '%'),
                                flat=True)
    history  = {}
    metadata = {}
    for name in names:
        history[name] = {}
        metadata[name] = {}
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'AND database_name="{}" '
                                            'AND table_name="{}" '
                                            'GROUP BY yr_mon '
                                            'ORDER BY yr_mon').format(instance, database, name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m"), row[1]])
        history[name]['year'] = instance_history
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'AND database_name="{}" '
                                            'AND table_name="{}" '
                                            'GROUP BY yr_mon_day '
                                            'ORDER BY yr_mon_day').format(instance, database, name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history[name]['month']   = instance_history[-32:]
        history[name]['week']    = instance_history[-8:]
        metadata[name]['passing'] = submit_query(('SELECT run_start_timestamp, check_violation_cnt '
                                                    'FROM check_results '
                                                    'WHERE instance_name="{}" '
                                                    'AND database_name="{}" '
                                                    'AND table_name="{}" '
                                                    'ORDER BY run_start_timestamp DESC '
                                                    'LIMIT 1').format(instance, database, name))[0][1]
        metadata[name]['yearlen'] = len(history[name]['year'])
        metadata[name]['monthlen'] = len(history[name]['month'])
        metadata[name]['weeklen'] = len(history[name]['week'])

    content = render_template('tables.html',
                                database=database,
                                instance=instance,
                                colors=colors,
                                names=names,
                                history=history,
                                metadata=metadata)
    return content


@app.route('/inspect/<instance>/<database>/<table>', methods=['GET', 'POST'])
def table(instance, database, table):
    get_all_names = lambda : submit_query(('SELECT DISTINCT check_name '
                                            'FROM check_results '
                                            'WHERE instance_name=? '
                                            'AND database_name=? '
                                            'AND table_name=?'),
                                    args=(instance, database, table), flat=True)
    names = get_all_names()
    if request.method == 'POST':
        if request.form['searchquery'] == '':
            names = get_all_names()
        else:
            names = submit_query(('SELECT DISTINCT table_name '
                                            'FROM check_results '
                                            'WHERE instance_name=? '
                                            'AND database_name=? '
                                            'AND table_name=? '
                                            'AND check_name LIKE ?'),
                                    args=(instance, database, table,
                                            '%' + request.form['searchquery'] + '%'),
                                    flat=True)
    history  = {}
    metadata = {}
    for name in names:
        history[name] = {}
        metadata[name] = {}
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'AND database_name="{}" '
                                            'AND table_name="{}" '
                                            'AND check_name="{}" '
                                            'GROUP BY yr_mon '
                                            'ORDER BY yr_mon').format(instance, database, table, name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m"), row[1]])
        history[name]['year'] = instance_history
        instance_history = []
        for row in submit_query(('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
                                            'FROM check_results '
                                            'WHERE instance_name="{}" '
                                            'AND database_name="{}" '
                                            'AND table_name="{}" '
                                            'AND check_name="{}" '
                                            'GROUP BY yr_mon_day '
                                            'ORDER BY yr_mon_day').format(instance, database, table, name)):
            instance_history.append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history[name]['month']   = instance_history[-32:]
        history[name]['week']    = instance_history[-8:]
        metadata[name]['passing'] = submit_query(('SELECT run_start_timestamp, check_violation_cnt '
                                                    'FROM check_results '
                                                    'WHERE instance_name="{}" '
                                                    'AND database_name="{}" '
                                                    'AND table_name="{}" '
                                                    'AND check_name="{}" '
                                                    'ORDER BY run_start_timestamp DESC '
                                                    'LIMIT 1').format(instance, database, table, name))[0][1]
        metadata[name]['yearlen'] = len(history[name]['year'])
        metadata[name]['monthlen'] = len(history[name]['month'])
        metadata[name]['weeklen'] = len(history[name]['week'])

    content = render_template('checks.html',
                                instance=instance,
                                database=database,
                                table=table,
                                colors=colors,
                                names=names,
                                history=history,
                                metadata=metadata)
    return content


@app.route('/inspect/<instance>/<database>/<table>/<check>', methods=['GET', 'POST'])
def checkdetails(instance, database, table, check):
    if request.method == 'POST':
        pass

    get_tables = lambda : submit_query(('SELECT DISTINCT(table_name) '
                                        'FROM check_results '
                                        'WHERE instance_name=? '
                                        'AND database_name=? '
                                        'AND table_name=? '
                                        'AND check_name=?'),
                                    args=(instance, database, table, check), flat=True)
    tables = get_tables()

    raw_history = submit_query(('SELECT * '
                                'FROM check_results '
                                'WHERE instance_name=? '
                                'AND database_name=? '
                                'AND table_name=? '
                                'AND check_name=?'),
                                args=(instance, database, table, check))

    history = {'year':[], 'month':[], 'week':[]}
    for row in submit_query(('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
                                'FROM check_results '
                                'WHERE instance_name=? '
                                'AND database_name=? '
                                'AND table_name=? '
                                'AND check_name=? '
                                'GROUP BY yr_mon '
                                'ORDER BY yr_mon'),
                                args=(instance, database, table, check)):
        history['year'].append([reformat_time(row[0], input_format="%Y-%m"), row[1]])
    for row in submit_query(('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
                                'FROM check_results '
                                'WHERE instance_name=? '
                                'AND database_name=? '
                                'AND table_name=? '
                                'AND check_name=? '
                                'GROUP BY yr_mon_day '
                                'ORDER BY yr_mon_day'),
                                args=(instance, database, table, check)):
        history['month'].append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history['week'].append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
    history['month'] = history['month'][-32:]
    history['week'] = history['week'][-8:]

    desc = 'Proin suscipit luctus orci placerat fringilla. Donec hendrerit laoreet risus eget adipiscing. Suspendisse in urna ligula, a volutpat mauris. Sed enim mi, bibendum eu pulvinar vel, sodales vitae dui. Pellentesque sed sapien lorem, at lacinia urna. In hac habitasse platea dictumst. Vivamus vel justo in leo laoreet ullamcorper non vitae lorem. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin bibendum ullamcorper rutrum.'

    content = render_template('checkdetails.html',
                        instance=instance,
                        database=database,
                        table=table,
                        check=check,
                        raw_history=raw_history,
                        tables=tables,
                        desc=desc,
                        colors=colors,
                        history=history)
    return content


if __name__ == '__main__':
    sys.exit(main())
