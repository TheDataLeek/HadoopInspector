#!/usr/bin/env python2
""" Generate demo test-results data. """
import os, sys
import random
import argparse
from os.path import join as pjoin
from os.path import isfile, isdir, exists
import csv

__version__ = '0.0.1'
__author__ = "Ken Farmer"

user_tables = \
    {'cust_type':
        {'cols':
            {'cust_type_id',
             'cust_type_name',
            },
         'mode': 'full',
         'partition_key': None,
         'partition_row_cnt_avg': 5,
         'checks':
            {'cust_type_uk':
                {'policy_type': 'quality',
                 'check_type':  'rule' },
             'cust_name_uk':
                {'policy_type': 'quality',
                 'check_type':  'rule' },
             'cust_not_empty':
                {'policy_type': 'quality',
                 'check_type':  'rule' },
            }
        }
    ,
   'asset_type':
        {'cols':
            {'asset_type_id',
             'asset_type_name',
            },
         'mode': 'full',
         'partition_key': None,
         'partition_row_cnt_avg': 5,
         'checks':
            {'asset_type_uk':
                {'policy_type': 'quality',
                 'check_type':  'rule' },
             'asset_name_uk':
                {'policy_type': 'quality',
                 'check_type':  'rule' },
             'asset_not_empty':
                {'policy_type': 'quality',
                 'check_type':  'rule' },
            }
        }
    }




def main():
    args = get_args()
    create_test_file(dirname='/tmp')




def create_test_file(dirname):
    assert isdir(dirname)
    fieldnames = ('instance_name', 'database_name', 'table_name',
                  'table_partitioned', 'run_start_timestamp', 'run_mode',
                  'partition_key', 'partition_value', 'check_name',
                  'check_policy_type', 'check_type',
                  'run_check_start_timestamp', 'run_check_end_timestamp',
                  'run_check_mode', 'run_check_rc', 'run_check_violation_cnt',
                  'run_check_anomaly_score', 'run_check_severity_score',
                  'run_check_validated')

    outfile = open(pjoin(dirname, 'inspector_demo.csv'), 'w')
    writer  = csv.DictWriter(outfile, fieldnames)
    # Establish instance name
    # Establish database name
    #for day_of_year in range(1,365):
    for month_of_year in range(1,13):
        for day_of_month in range(1, get_month_days(month_of_year)+1):
            run_start_timestamp = 0
            run_mode            = 0
            print('2015-%-2.2d-%-2.2d' % (month_of_year, day_of_month))
            for table_name in user_tables:
                print('    table_name: %s' % table_name)
                for check_name in user_tables[table_name]['checks']:
                    print('        check_name: %s' % check_name)
                    row_dict = {}
                    row_dict['instance_name']             = get_instance_name()
                    row_dict['database_name']             = get_database_name()
                    row_dict['table_name']                = table_name
                    row_dict['table_partitioned']         = get_table_partitioned(user_tables[table_name]['partition_key'])
                    row_dict['run_start_timestamp']       = get_run_start_timestamp(month_of_year, day_of_month)
                    row_dict['run_mode']                  = 'full'
                    row_dict['partition_key']             = get_partition_key(user_tables[table_name]['partition_key'])
                    row_dict['partition_value']           = ''
                    row_dict['check_name']                = check_name
                    row_dict['check_policy_type']         = get_check_policy_type(user_tables[table_name]['checks'][check_name]['policy_type'])
                    row_dict['check_type']                = get_check_type(user_tables[table_name]['checks'][check_name]['check_type'])
                    row_dict['run_check_start_timestamp'] = row_dict['run_start_timestamp']
                    row_dict['run_check_end_timestamp']   = row_dict['run_start_timestamp']
                    row_dict['run_check_mode']            = 'full'
                    row_dict['run_check_rc']              = '0'
                    row_dict['run_check_violation_cnt']   = '0'
                    row_dict['run_check_anomaly_score']   = '0'
                    row_dict['run_check_severity_score']  = '0'
                    row_dict['run_check_validated']       = ''
                    writer.writerow(row_dict)
    outfile.close()




def get_month_days(month_of_year):
    if month_of_year in (1,3,5,7,8,10,12):
        return 31
    elif month_of_year in (2,):
        return 28
    else:
        return 30


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

def get_run_start_timestamp(month, day):
    ts_str = '2015-%d-%d 02:00:00' % (month, day)
    return ts_str


def get_run_violation_cnt():
    return 0

def get_check_policy_type(raw_check_policy_type):
    return raw_check_policy_type

def get_check_type(raw_check_type):
    return raw_check_type




def get_args():
    parser = argparse.ArgumentParser(description='Gets information about directory')
    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='displays version number')
    parser.add_argument('--long-help',
                        help='Provides more verbose help')

    args = parser.parse_args()
    if args.long_help:
        print(__doc__)
        sys.exit(0)

    return vars(args)


if __name__ == '__main__':
   sys.exit(main())