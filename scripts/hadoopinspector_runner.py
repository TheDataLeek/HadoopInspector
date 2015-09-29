#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""



import sys
import os
import argparse
import logging
import collections
import subprocess
import json
from pprint import pprint as pp
from os.path import dirname, basename, isdir, isfile, exists
from os.path import join as pjoin

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import hadoopinspector.core as core



def main():
    args       = get_args()
    configure_logger(args.log_dir)

    registry   = core.Registry()
    registry.load_registry(args.registry_filename)
    registry.generate_db_registry(args.instance, args.database, args.table, args.check)

    check_repo     = core.CheckRepo(args.check_dir)
    check_results  = core.CheckResults(db_fqfn=args.results_filename)

    checker    = core.CheckRunner(registry, check_repo, check_results, args.instance, args.database)
    checker.add_db_var('hapinsp_instance', args.instance)
    checker.add_db_var('hapinsp_database', args.database)
    checker.run_checks_for_tables(args.table)

    if args.report:
        for rec in checker.results.get_formatted_results(args.detail_report):
            print(rec)

    sys.exit(checker.results.get_max_rc())




def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--instance',
                        required=True,
                        help='specifies name of instance to check')
    parser.add_argument('--database',
                        required=True,
                        help='specifies name of database to check')
    parser.add_argument('--table',
                        required=False,
                        help='specifies a single table to test against')
    parser.add_argument('--check',
                        required=False,
                        help='specifies a single check to test against')
    parser.add_argument('-r', '--report',
                        action='store_true',
                        default=False,
                        help='indicates that a basic report should be generated')
    parser.add_argument('--detail-report',
                        action='store_true',
                        default=False,
                        help='indicates that a detailed report should be generated')
    parser.add_argument('--registry-filename',
                        required=True,
                        help='registry file contains check config')
    parser.add_argument('--results-filename',
                        required=True,
                        help='results sqlite file')
    parser.add_argument('--check-dir',
                        required=True,
                        help='which directory to look for the tests in')
    parser.add_argument('--log-dir',
                        required=True,
                        help='specifies directory to log output to')

    args = parser.parse_args()

    if not args.check_dir or not isdir(args.check_dir):
        print('Supplied test directory does not exist. Please create.')
        sys.exit(1)
    if not isdir(args.log_dir):
        print('Supplied log directory does not exist.  Please create.')
        sys.exit(1)
    if not isfile(args.registry_filename):
        print('Supplied registry-filename does not exist.  Please correct.')
        sys.exit(1)
    if args.detail_report:
        args.report = True

    return args



def configure_logger(logdir):
    assert isdir(logdir)
    logfile = pjoin(logdir, 'runner.log')
    logging.basicConfig(filename=logfile,
                        level=logging.DEBUG)


if __name__ == '__main__':
    sys.exit(main())
