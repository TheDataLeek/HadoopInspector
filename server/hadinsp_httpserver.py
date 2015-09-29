#!/usr/bin/env python2

"""
    This source code is protected by the BSD license.  See the file "LICENSE"
    in the source code root directory for the full language or refer to it here:
    http://opensource.org/licenses/BSD-3-Clause
    Copyright 2015 Will Farmer and Ken Farmer
"""

import sys, os
import json
import re
import sqlite3
import datetime
from flask import Flask, render_template, Markup, request


app = Flask(__name__)

#TODO: Actually use config and determine configurable fields
config = None

colors = [
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
    global config
    config = get_config()
    app.run(host='localhost',
            port=config['port'],
            debug=True)

def get_config():
    """
    Find config.json wherever it may live
    """
    config = FrontEnd()
    config_file = config.find_file('.', 'config.json') or config.find_file('..', 'config.json')  # shortcircuit to find in current or above.
    if config_file is None:
        print('Missing Config File. Aborting.')
        sys.exit(0)
    else:
        with open(config_file, 'r') as f:
            config = json.loads(f.read())
            return config


class FrontEnd(object):
    def __init__(self):
        self.python2 = '2.7' in sys.version
        self.python3 = not self.python2

    def find_file(self, path, file_to_find):
        for dirname, dirnames, filenames in os.walk(path):
            for filename in filenames:
                if filename == file_to_find:
                    return os.path.join(dirname, filename)

    def get_database(self):
        database_file = self.find_file('.', config['db']) or self.find_file('..', config['db'])
        if database_file is None:
            print("Missing Database")
            sys.exit(0)
        return database_file

    def submit_query(self, query_string, args=None, flat=False):
        database_file = self.get_database()
        connection = sqlite3.connect(database_file)
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
                return self.flatten(results)
        else:
            return results

    def flatten(self, l):
        return [item for sublist in l for item in sublist]

    def reformat_time(self, s, input_format='%Y-%m-%d %H:%M:%S', output_format='%Y-%m-%d'):
        return datetime.datetime.strftime(datetime.datetime.strptime(s, input_format), output_format)

    def clean_strings(self, strings):
        new_strings = [re.sub(ur'-', u'_', s, re.UNICODE) for s in strings]
        return new_strings

    def get_row_names(self, method, form, query, query_args=None, search_query=None, search_args=None):
        # First get all names
        if query_args:
            names = self.submit_query(query, args=query_args, flat=True)
        else:
            names = self.submit_query(query, flat=True)
        # Apply search if applicable.
        if method == 'POST':
            # If empty search, reset to full
            if form['searchquery'] == '':
                names = self.submit_query(query, flat=True)
            else:
                names = self.submit_query(search_query, args=search_args, flat=True)
        clean_names = self.clean_strings(names)
        return names, clean_names

    def get_all_data(self, names, clean_names, month_query, day_query, passing_query):
        # Now get history in nested dict.
        history  = {}
        metadata = {}
        for i in range(len(names)):
            name                 = names[i]
            clean_name           = clean_names[i]
            history[clean_name]  = {}
            metadata[clean_name] = {}
            row_history          = []
            for row in self.submit_query(month_query.format(name)):
                row_history.append([self.reformat_time(row[0], input_format="%Y-%m"), row[1]])
            history[clean_name]['year'] = row_history
            row_history = []
            for row in self.submit_query(day_query.format(name)):
                row_history.append([self.reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
            history[clean_name]['month']   = row_history[-32:]
            history[clean_name]['week']    = row_history[-8:]
            try:
                metadata[clean_name]['passing'] = self.submit_query(passing_query.format(name))[0][1]
            except IndexError:
                # If no tests have been run, return 0, or "Passing"
                metadata[clean_name]['passing'] = 0
            metadata[clean_name]['yearlen'] = len(history[clean_name]['year'])
            metadata[clean_name]['monthlen'] = len(history[clean_name]['month'])
            metadata[clean_name]['weeklen'] = len(history[clean_name]['week'])
        return history, metadata


@app.route('/', methods=['GET', 'POST'])
def root():
    data_gen = FrontEnd()
    try:
        search_form_query = request.form['searchquery']
    except KeyError:
        search_form_query = ''

    names, clean_names = data_gen.get_row_names(request.method, request.form,
        'SELECT DISTINCT instance_name FROM check_results',
        search_query=('SELECT DISTINCT(instance_name) '
                            'FROM check_results '
                            'WHERE instance_name LIKE ?'),
        search_args=('%' + search_form_query + '%',))

    history, metadata = data_gen.get_all_data(names, clean_names,
        ('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_type NOT LIKE "setup_%" '
            'GROUP BY yr_mon '
            'ORDER BY yr_mon'),
        ('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_type NOT LIKE "setup_%" '
            'GROUP BY yr_mon_day '
            'ORDER BY yr_mon_day'),
        ('SELECT run_start_timestamp, check_violation_cnt '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_type NOT LIKE "setup_%" '
            'ORDER BY run_start_timestamp DESC '
            'LIMIT 1'))

    content = render_template('instances.html',
                        colors=colors,
                        names=names,
                        clean_names=clean_names,
                        history=history,
                        metadata=metadata)
    return content


@app.route('/inspect/<instance>', methods=['GET', 'POST'])
def instance(instance):
    data_gen = FrontEnd()
    try:
        search_form_query = request.form['searchquery']
    except KeyError:
        search_form_query = ''

    names, clean_names = data_gen.get_row_names(request.method, request.form,
        ('SELECT DISTINCT(database_name) '
            'FROM check_results '
            'WHERE instance_name=?'),
        (instance,),
        ('SELECT DISTINCT(database_name) '
            'FROM check_results '
            'WHERE instance_name=? '
            'AND database_name LIKE ?'),
        (instance, '%' + search_form_query + '%'))

    history, metadata = data_gen.get_all_data(names, clean_names,
        (('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '.format(instance)) +
            ('AND check_type NOT LIKE "setup_%" '
            'AND database_name="{}" '
            'GROUP BY yr_mon '
            'ORDER BY yr_mon')),
        (('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '.format(instance)) +
            ('AND check_type NOT LIKE "setup_%" '
            'AND database_name="{}" '
            'GROUP BY yr_mon_day '
            'ORDER BY yr_mon_day')),
        (('SELECT run_start_timestamp, check_violation_cnt '
            'FROM check_results '
            'WHERE instance_name="{}" '.format(instance)) +
            ('AND check_type NOT LIKE "setup_%" '
            'AND database_name="{}" '
            'ORDER BY run_start_timestamp DESC '
            'LIMIT 1')))

    content = render_template('databases.html',
                                instance=instance,
                                colors=colors,
                                names=names,
                                clean_names=clean_names,
                                history=history,
                                metadata=metadata)
    return content


@app.route('/inspect/<instance>/<database>', methods=['GET', 'POST'])
def database(instance, database):
    data_gen = FrontEnd()
    try:
        search_form_query = request.form['searchquery']
    except KeyError:
        search_form_query = ''

    names, clean_names = data_gen.get_row_names(request.method, request.form,
        ('SELECT DISTINCT table_name '
            'FROM check_results '
            'WHERE instance_name=? '
            'AND database_name=?'),
        (instance, database),
        ('SELECT DISTINCT table_name '
            'FROM check_results '
            'WHERE instance_name=? '
            'AND database_name=? '
            'AND table_name LIKE ?'),
        (instance, database, '%' + search_form_query + '%'))

    history, metadata = data_gen.get_all_data(names, clean_names,
        (('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_type NOT LIKE "setup_%" '
            'AND database_name="{}" '.format(instance, database)) +
            ('AND table_name="{}" '
            'GROUP BY yr_mon '
            'ORDER BY yr_mon')),
        (('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_type NOT LIKE "setup_%" '
            'AND database_name="{}" '.format(instance, database)) +
            ('AND table_name="{}" '
            'GROUP BY yr_mon_day '
            'ORDER BY yr_mon_day')),
        (('SELECT run_start_timestamp, check_violation_cnt '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_type NOT LIKE "setup_%" '
            'AND database_name="{}" '.format(instance, database)) +
            ('AND table_name="{}" '
            'ORDER BY run_start_timestamp DESC '
            'LIMIT 1')))

    content = render_template('tables.html',
                                database=database,
                                instance=instance,
                                colors=colors,
                                names=names,
                                clean_names=clean_names,
                                history=history,
                                metadata=metadata)
    return content


@app.route('/inspect/<instance>/<database>/<table>', methods=['GET', 'POST'])
def table(instance, database, table):
    data_gen = FrontEnd()
    try:
        search_form_query = request.form['searchquery']
    except KeyError:
        search_form_query = ''

    names, clean_names = data_gen.get_row_names(request.method, request.form,
        ('SELECT DISTINCT check_name '
            'FROM check_results '
            'WHERE instance_name=? '
            'AND database_name=? '
            'AND check_name NOT LIKE "setup_%" '
            'AND table_name=?'),
        (instance, database, table),
        ('SELECT DISTINCT table_name '
            'FROM check_results '
            'WHERE instance_name=? '
            'AND database_name=? '
            'AND table_name=? '
            'AND check_name NOT LIKE "setup_%" '
            'AND check_name LIKE ?'),
        (instance, database, table, '%' + search_form_query + '%'))

    history, metadata = data_gen.get_all_data(names, clean_names,
        (('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_name NOT LIKE "setup_%" '
            'AND database_name="{}" '
            'AND table_name="{}" '.format(instance, database, table)) +
            ('AND check_name="{}" '
            'GROUP BY yr_mon '
            'ORDER BY yr_mon')),
        (('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_name NOT LIKE "setup_%" '
            'AND database_name="{}" '
            'AND table_name="{}" '.format(instance, database, table)) +
            ('AND check_name="{}" '
            'GROUP BY yr_mon_day '
            'ORDER BY yr_mon_day')),
        (('SELECT run_start_timestamp, check_violation_cnt '
            'FROM check_results '
            'WHERE instance_name="{}" '
            'AND check_name NOT LIKE "setup_%" '
            'AND database_name="{}" '
            'AND table_name="{}" '.format(instance, database, table)) +
            ('AND check_name="{}" '
            'ORDER BY run_start_timestamp DESC '
            'LIMIT 1')))

    content = render_template('checks.html',
                                instance=instance,
                                database=database,
                                table=table,
                                colors=colors,
                                names=names,
                                clean_names=clean_names,
                                history=history,
                                metadata=metadata)
    return content


@app.route('/inspect/<instance>/<database>/<table>/<check>', methods=['GET', 'POST'])
def checkdetails(instance, database, table, check):
    data_gen = FrontEnd()
    raw_history = data_gen.submit_query(('SELECT * '
                                'FROM check_results '
                                'WHERE instance_name=? '
                                'AND database_name=? '
                                'AND table_name=? '
                                'AND check_name=?'),
                                args=(instance, database, table, check))

    get_tables = lambda : data_gen.submit_query(('SELECT DISTINCT(table_name) '
                                        'FROM check_results '
                                        'WHERE instance_name=? '
                                        'AND database_name=? '
                                        'AND table_name=? '
                                        'AND check_name=?'),
                                    args=(instance, database, table, check), flat=True)
    tables = get_tables()

    history = {'year':[], 'month':[], 'week':[]}
    for row in data_gen.submit_query(('SELECT strftime("%Y-%m", run_start_timestamp) as yr_mon, SUM(check_violation_cnt) as tot '
                                'FROM check_results '
                                'WHERE instance_name=? '
                                'AND database_name=? '
                                'AND table_name=? '
                                'AND check_name=? '
                                'GROUP BY yr_mon '
                                'ORDER BY yr_mon'),
                                args=(instance, database, table, check)):
        history['year'].append([data_gen.reformat_time(row[0], input_format="%Y-%m"), row[1]])
    for row in data_gen.submit_query(('SELECT strftime("%Y-%m-%d", run_start_timestamp) as yr_mon_day, SUM(check_violation_cnt) as tot '
                                'FROM check_results '
                                'WHERE instance_name=? '
                                'AND database_name=? '
                                'AND table_name=? '
                                'AND check_name=? '
                                'GROUP BY yr_mon_day '
                                'ORDER BY yr_mon_day'),
                                args=(instance, database, table, check)):
        history['month'].append([data_gen.reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history['week'].append([data_gen.reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
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
