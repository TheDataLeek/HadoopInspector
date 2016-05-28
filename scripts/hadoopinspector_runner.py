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
import logging.handlers
import collections
import subprocess
import json
from pprint import pprint as pp
from os.path import dirname, basename, isdir, isfile, exists
from os.path import join as pjoin

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import hadoopinspector.core as core

runner_logger = None


def main():
    global runner_logger
    args = get_args()
    runner_logger = setup_runner_logger(args.log_dir, args.log_level, args.log_to_console)
    runner_logger.info("runner starting now")

    registry = core.Registry()
    registry.load_registry(args.registry_filename)
    registry.generate_db_registry(args.instance, args.database, args.table, args.check)

    check_repo = core.CheckRepo(args.check_dir)
    check_results = core.CheckResults(db_fqfn=args.results_filename)

    checker = core.CheckRunner(registry, check_repo, check_results, args.instance, args.database,
                               args.log_dir, args.log_level)
    checker.add_db_var('hapinsp_instance', args.instance)
    checker.add_db_var('hapinsp_database', args.database)
    checker.add_db_var('hapinsp_ssl',      args.ssl)
    checker.run_checks_for_tables(args.table)

    if args.report:
        print('')
        print("===================== final report =======================")
        for rec in checker.results.get_formatted_results(args.detail_report):
            fields = rec.split('|')
            print('{tab:<30} {rule:<40} {mode:<12} {rc:<7} {cnt:<7}'.format(tab=fields[0], rule=fields[1],
                         mode=(fields[2] or 'unk'), rc=(fields[3] or 'unk'), cnt=(fields[4] or 0) ) )
        print('')

    runner_logger.info("runner terminating now with rc: %s", checker.results.get_max_rc())
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
    parser.add_argument('--ssl',
                        action='store_true',
                        dest='ssl')
    parser.add_argument('--no-ssl',
                        action='store_false',
                        dest='ssl')
    parser.add_argument('--console-log',
                        action='store_true',
                        dest='log_to_console')
    parser.add_argument('--no-console-log',
                        action='store_false',
                        dest='log_to_console')
    parser.add_argument('--log-level',
                        default='debug',
                        choices=['debug', 'info', 'warning', 'error', 'critical'])

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
    if args.ssl is None:
        args.ssl = False

    return args



def setup_runner_logger(logdir, log_level, log_to_console):
    assert isdir(logdir)
    assert log_level in ('debug', 'info', 'warning', 'error', 'critical')
    assert log_to_console in (True, False)
    log_filename = pjoin(logdir, 'runner.log')

    #--- create logger
    runner_logger = logging.getLogger('RunnerLogger')
    runner_logger.setLevel(log_level.upper())

    #--- add formatting:
    log_format = '%(asctime)s : %(name)-12s : %(levelname)-8s : %(message)s'
    date_format = '%Y-%m-%d %H.%M.%S'
    runner_formatter = logging.Formatter(log_format, date_format)

    #--- create rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000, backupCount=20)
    file_handler.setFormatter(runner_formatter)
    runner_logger.addHandler(file_handler)

    #--- create console handler:
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(runner_formatter)
        runner_logger.addHandler(console_handler)

    #--- ensure any uncaught exceptions get logged:
    sys.excepthook = excepthook

    return runner_logger


def excepthook(*args):
    runner_logger.critical('uncaught exception - exiting now', exc_info=args)
    sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
