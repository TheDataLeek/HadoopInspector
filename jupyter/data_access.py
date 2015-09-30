#!/usr/bin/env python3

import sys, os
import json

def get_config():
    """
    Find config.json wherever it may live
    """
    config_file = find_file('.', 'config.json') or find_file('..', 'config.json')  # shortcircuit to find in current or above.
    if config_file is None:
        print('Missing Config File. Aborting.')
        return None
    else:
        with open(config_file, 'r') as f:
            config = json.loads(f.read())
            return config

def find_file(path, file_to_find):
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename == file_to_find:
                return os.path.join(dirname, filename)

def get_database():
    config = get_config()
    database_file = find_file('.', config['db']) or find_file('..', config['db'])
    if database_file is None:
        print("Missing Database")
        return None
    return database_file

def submit_query(query_string, args=None, flat=False):
    config = get_config()
    database_file = get_database(config)
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
            return flatten(results)
    else:
        return results

def flatten(l):
    return [item for sublist in l for item in sublist]

def reformat_time(s, input_format='%Y-%m-%d %H:%M:%S', output_format='%Y-%m-%d'):
    return datetime.datetime.strftime(datetime.datetime.strptime(s, input_format), output_format)

def clean_strings(strings):
    new_strings = [re.sub('-', '_', s) for s in strings]
    return new_strings

def get_row_names(method, form, query, query_args=None, search_query=None, search_args=None):
    # First get all names
    if query_args:
        names = submit_query(query, args=query_args, flat=True)
    else:
        names = submit_query(query, flat=True)
    # Apply search if applicable.
    if method == 'POST':
        # If empty search, reset to full
        if form['searchquery'] == '':
            names = submit_query(query, flat=True)
        else:
            names = submit_query(search_query, args=search_args, flat=True)
    clean_names = clean_strings(names)
    return names, clean_names

def get_all_data(names, clean_names, month_query, day_query, passing_query):
    # Now get history in nested dict.
    history  = {}
    metadata = {}
    for i in range(len(names)):
        name                 = names[i]
        clean_name           = clean_names[i]
        history[clean_name]  = {}
        metadata[clean_name] = {}
        row_history          = []
        for row in submit_query(month_query.format(name)):
            row_history.append([reformat_time(row[0], input_format="%Y-%m"), row[1]])
        history[clean_name]['year'] = row_history
        row_history = []
        for row in submit_query(day_query.format(name)):
            row_history.append([reformat_time(row[0], input_format="%Y-%m-%d"), row[1]])
        history[clean_name]['month']   = row_history[-32:]
        history[clean_name]['week']    = row_history[-8:]
        try:
            metadata[clean_name]['passing'] = submit_query(passing_query.format(name))[0][1]
        except IndexError:
            # If no tests have been run, return 0, or "Passing"
            metadata[clean_name]['passing'] = 0
        metadata[clean_name]['yearlen'] = len(history[clean_name]['year'])
        metadata[clean_name]['monthlen'] = len(history[clean_name]['month'])
        metadata[clean_name]['weeklen'] = len(history[clean_name]['week'])
    return history, metadata
