#!/usr/bin/env python2
""" Generate demo test-results data.

    This program is used to create fake results from daily runs against a
    hypothetical user database called "Westwind".  This hypothetical
    database has the following tables within it:
       - cust_type - a small, non-partitioned lookup table with 5 rows
       - asset_type - a small, non-partitioned lookup table with 5 rows
       - cust_asset_events - a large, partitioned fact table

    There are three tests for the two lookup tables, and six for the large
    fact table that run daily.  The program generates one run for every day
    from 2015-01-01 to 2015-12-31.  A small random number of tests will find
    violations.  The user tables that this demo creates tests for include:
        - cust_type                (~ 5 rows)
        - asset_type               (~ 5 rows)
        - event_type               (~ 5 rows)
        - dates                    (~ 3650 rows)
        - locations                (tbd)
        - customers                (~ 10,000 rows)
        - assets                   (~ 10,000,000 rows)
        - cust_asset_events        (~ 3,000,000,000 rows) = avg of 1 event/asset/day for 7 years
        - cust_asset_event_month   (~ 840,000,000 rows) = 1 row/asset/month for 7 years

    This results data will be written by default to /tmp/inspector_demo.sqlite.
    4380 records will be written to this comma-delimited csv file without
    any quotes.  The fields are in the following order:
        - instance_name             STRING    (always 'prod')
        - database_name             STRING    (always 'westwind')
        - table_name                STRING
        - run_start_timestamp       TIMESTAMP (YYYY-MM-DD HH:MM:SS)
        - check_name                STRING
        - check_policy_type         STRING    ('quality' or 'data-management')
        - check_type                STRING    ('rule')
        - run_check_mode            STRING    ('full' or 'incremental')
        - run_check_rc              INTEGER   (always 0)
        - run_check_violation_cnt   INTEGER
        - run_check_scope           INTEGER   (0-100)
        - run_check_unit            STRING    ('tables' or 'rows')
        - run_check_severity_score  INTEGER   (0-100)

    These fields were initially included but have been taken out for now:
        #- table_partitioned         INTEGER   (1=True, 0=False)
        #- run_mode                  STRING    ('full' or 'incremental')
        #- partition_key             STRING    ('' or 'date_id')
        #- partition_value           STRING    (5-digit julian date for partitioned tables)
        #- run_check_start_timestamp TIMESTAMP (YYYY-MM-DD HH:MM:SS)
        #- run_check_end_timestamp   TIMESTAMP (YYYY-MM-DD HH:MM:SS)
        #- run_check_anomaly_score   INTEGER   (always 0)


This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""
from __future__ import division
import os, sys
import random
import argparse
import time, datetime
from os.path import join as pjoin
from os.path import isfile, isdir, exists, dirname, basename
import csv
import random
import json
from pprint import pprint as pp

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import hadoopinspector.core as core

__version__ = '0.0.2'
user_tables = {}


def main():
    global user_tables
    args = get_args()
    if args.dirname:
        fn = pjoin(args.dirname, 'instance-%s_db-%s_config.json' % (args.user_instance, args.user_db))
    else:
        fn = 'instance-%s_db-%s_config.json' % (args.user_instance, args.user_db)
    with open(fn, 'r') as f:
        user_tables = json.load(f)
    create_test_file(args.user_instance, args.user_db, args.outfile)
    print('Demo file created: %s' % args.outfile)



def create_test_file(user_instance, user_db, output_filename):

    for month_of_year in range(1,13):
        print("generating month: %d" % month_of_year)
        for day_of_month in range(1, get_month_days(month_of_year)+1):
            run_start_datetime   = get_run_start_datetime(2014, month_of_year, day_of_month)
            check_results = core.CheckResults(output_filename)
            for table_name in user_tables:
                for check_name in user_tables[table_name]['checks']:
                    #curr_datetime                         = get_curr_datetime(curr_datetime, run_start_datetime)
                    #row_dict['table_partitioned']         = get_table_partitioned(user_tables[table_name]['partition_key'])
                    #row_dict['run_start_timestamp']       = get_ts_from_dt(run_start_datetime)
                    #row_dict['run_mode']                  = user_tables[table_name]['mode']
                    #row_dict['partition_key']             = get_partition_key(user_tables[table_name]['partition_key'])
                    #row_dict['partition_value']           = get_partition_value(row_dict['run_mode'], curr_datetime)
                    #row_dict['run_check_start_timestamp'] = get_ts_from_dt(curr_datetime)
                    #row_dict['run_check_end_timestamp']   = get_ts_from_dt(curr_datetime + datetime.timedelta(seconds=1))

                    default_severity_name  = user_tables[table_name]['checks'][check_name]['severity']
                    default_severity_score = convert_severity(default_severity_name)
                    scope =  get_scope(user_tables[table_name]['partition_key'],
                                    user_tables[table_name]['partition_row_cnt_avg'],
                                    user_tables[table_name]['checks'][check_name]['violation_unit'],
                                    get_violation_cnt(table_name, check_name))

                    check_results.add(user_instance,
                            user_db,
                            table_name,
                            check_name,
                            get_violation_cnt(table_name, check_name),
                            '0',
                            'active',
                            get_check_type(user_tables[table_name]['checks'][check_name]['check_type']),
                            get_check_policy_type(user_tables[table_name]['checks'][check_name]['policy_type']),
                            get_run_check_mode(user_tables[table_name]['mode'],
                                            user_tables[table_name]['checks'][check_name]['mode']),
                            user_tables[table_name]['checks'][check_name]['violation_unit'],
                            scope,
                            get_severity_score(default_severity_score, scope),
                            run_start_timestamp=run_start_datetime,
                            run_stop_timestamp=(run_start_datetime + datetime.timedelta(hours=1))
                            )

            check_results.write_to_sqlite()


def get_violation_cnt(table_name, check_name):
    mode           = user_tables[table_name]['mode']
    partition_key  = user_tables[table_name]['partition_key']
    avg_row_cnt    = user_tables[table_name]['partition_row_cnt_avg']
    violation_unit = user_tables[table_name]['checks'][check_name]['violation_unit']
    failure_rate   = user_tables[table_name]['checks'][check_name].get('failure_rate', 0.1)

    if random.random() < failure_rate:
        if violation_unit == 'table':
            return 1
        else:
            if partition_key is None and random.random() < 0.5:
                # screwed up the all rows, probably because you reloaded 100% of
                # the data wrong.  Maybe due to:
                #    - loaded same data twice
                #    - failed to load
                return int(avg_row_cnt)
            else:
                # screwed up a subset of rows, probably a subset of a single
                # partition
                return int(avg_row_cnt * random.random())
    else:
        return 0


def get_partition_value(run_mode, curr_datetime):
    if run_mode is None:
        return None
    else:
        tt = curr_datetime.timetuple()
        return '%d%03d' % (tt.tm_year, tt.tm_yday)


def get_run_check_mode(run_mode, check_mode):
    if run_mode == 'incremental' and check_mode == 'incremental':
        return 'incremental'
    else:
        return 'full'


def convert_severity(in_val):
    if in_val is None:
        return 0
    elif in_val == 'low':
        return 10
    elif in_val == 'medium':
        return 50
    elif in_val == 'high':
        return 100
    else:
        print('error: invalid convert_severity in_val: %s' % in_val)
        sys.exit(1)


def get_scope(partition_key, avg_row_cnt, violation_unit, violation_cnt):
    """ Returns the scope of the violations.
        Range is 0 to 100 - 100 being the greatest scope
    """
    assert isnumeric(violation_cnt)
    assert isnumeric(avg_row_cnt)
    if not violation_cnt:
        return 0
    elif violation_unit == 'tables':
        return 100
    else:
        if partition_key is None:
            return (violation_cnt / avg_row_cnt) * 100
        else:
            # assumes 100 partitions, or maybe more but recent
            # data is more valuable
            return (violation_cnt / avg_row_cnt)


def get_severity_score(default_severity_score, scope):
    """ Returns the severity of the violations.
        Range is 0 to 100 - 100 being the most severe.
    """
    assert isnumeric(default_severity_score)
    assert isnumeric(scope)
    severity_score = (scope * default_severity_score) / 100
    if severity_score == 0:
        return 0
    else:
        return int(max(1, min(100, severity_score)))


def get_partition_key(raw_partition_key):
    if raw_partition_key is None:
        return ''
    else:
        return raw_partition_key


def get_instance_name():
    return 'prod'


def get_database_name():
    return 'westwind'


def get_table_partitioned(partition_key):
    if partition_key is not None and partition_key != '':
        return '1'
    else:
        return '0'

def get_run_start_datetime(year, month, day):
    hour    = 2
    minute  = 0
    second  = 0
    d = datetime.datetime(2015, month, day, hour, minute, second)
    return d

def get_ts_from_dt(dt):
    ts_format  = '%Y-%m-%d %H:%M:%S'
    return dt.strftime(ts_format)

def get_curr_datetime(curr_datetime, run_start_datetime):
    if curr_datetime is None:
        # first time used
        return run_start_datetime
    elif curr_datetime > run_start_datetime:
        # after the first tie for a day
        return curr_datetime + datetime.timedelta(seconds=1)
    else:
        # first time for a day
        return run_start_datetime


def get_month_days(month_of_year):
    if month_of_year in (1,3,5,7,8,10,12):
        return 31
    elif month_of_year in (2,):
        return 28
    else:
        return 30


def get_check_policy_type(raw_check_policy_type):
    return raw_check_policy_type


def get_check_type(raw_check_type):
    return raw_check_type


def isnumeric(val):
    try:
        int(val)
    except TypeError:
        return False
    except ValueError:
        return False
    else:
        return True


def get_args():
    parser = argparse.ArgumentParser(description='Generates demo data')
    parser.add_argument('outfile',
                        help='output file to append to')
    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='displays version number')
    parser.add_argument('--long-help',
                        action='store_true',
                        help='Provides more verbose help')
    parser.add_argument('--user-instance',
                        choices=['prod', 'prodfailover', 'staging', 'dev'],
                        required=True)
    parser.add_argument('--user-db',
                        choices=['AssetUserEvents'],
                        required=True)
    parser.add_argument('--dirname',
                        help='path to config files')

    args = parser.parse_args()
    if args.long_help:
        print(__doc__)
        sys.exit(0)

    return args


if __name__ == '__main__':
   sys.exit(main())

