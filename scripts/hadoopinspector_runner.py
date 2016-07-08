#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""
import sys, os, argparse
import logging
import logging.handlers
from pprint import pprint as pp
from os.path import dirname, basename, isdir, isfile, exists
from os.path import join as pjoin

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
from hadoopinspector._version import __version__
import hadoopinspector.core as core
import hadoopinspector.registry as registry
import hadoopinspector.check_runner as check_engine
import hadoopinspector.check_results as chk_results

runner_logger = None


def main():
    global runner_logger
    args = get_args()
    runner_logger = setup_runner_logger(args.log_dir, args.log_level, args.log_to_console)
    runner_logger.info("runner starting now")
    runner_logger.info("instance: %s", args.instance)
    runner_logger.info("database: %s", args.database)
    runner_logger.info("log_level: %s", args.log_level)
    if args.user_table_vars:
        runner_logger.info("user table vars: %s", args.user_table_vars)

    reg = registry.Registry()
    reg.load_registry(args.registry_filename)
    reg.default()
    reg.filter_registry(args.table, args.check)
    reg.validate()

    check_repo = core.CheckRepo(args.check_dir)
    check_results = chk_results.CheckResults(args.instance, args.database, db_fqfn=args.results_filename)

    checker = check_engine.CheckRunner(reg, check_repo, check_results, args.instance, args.database,
                                       args.log_dir, args.log_level, args.user_table_vars)
    checker.add_db_var('hapinsp_instance', args.instance)
    checker.add_db_var('hapinsp_database', args.database)
    checker.add_db_var('hapinsp_ssl',      args.ssl)
    checker.run_checks_for_tables()

    if args.report:
        print('')
        print("===================== final report =======================")
        print('{tab:<{twidth}.{twidth}} {rule:<{rwidth}.{rwidth}} {mode:<12} {rc:<7} {cnt:<7} {data_start:<20} {data_stop:<20}'.format(twidth=40, rwidth=40, tab='table',  rule='rule',
                         mode='mode', rc='rc', cnt='cnt', data_start='data_start', data_stop='data_stop') )
        print('')
        for rec in checker.results.get_formatted_results(args.detail_report):
            fields = rec.split('|')
            print('{tab:<{twidth}.{twidth}} {rule:<{rwidth}.{rwidth}} {mode:<12} {rc:<7} {cnt:<7} {data_start:<20} {data_stop:<20}'.format(twidth=40, rwidth=40, tab=fields[0], rule=fields[1],
                         mode=(fields[2] or 'unk'), rc=(fields[3] or 'unk'), cnt=(fields[4] or 0), data_start=fields[6], data_stop=fields[7]) )
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
    parser.add_argument('--user-table-vars',
                        default=None,
                        nargs='*',
                        help='add space-delimited key-values after any setup checks')
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
    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='displays version number')

    args = parser.parse_args()

    if not isdir(args.check_dir):
        parser.error('Supplied check directory does not exist. Please correct.')
    if not args.check_dir:
        parser.error('Check directory was not provied. Please provide.')
    if not isdir(args.log_dir):
        parser.error('Supplied log directory does not exist.  Please create.')
    if not isfile(args.registry_filename):
        parser.error('Supplied registry-filename does not exist.  Please correct.')
    if args.detail_report:
        args.report = True
    if args.ssl is None:
        args.ssl = False
    if args.user_table_vars is not None:
        kw_dict = {}
        for kw_str in args.user_table_vars:
            kw_parts = kw_str.split('=')
            if len(kw_parts) != 2:
                parser.error('Invalid user_args: must be space-delimited list of key=value')
            kw_dict[kw_parts[0]] = kw_parts[1]
        args.user_table_vars = kw_dict

    return args



def setup_runner_logger(logdir, log_level, log_to_console):
    assert isdir(logdir)
    assert log_level in ('debug', 'info', 'warning', 'error', 'critical')
    assert log_to_console in (True, False)
    log_filename = pjoin(logdir, 'runner.log')

    #--- create logger
    logger = logging.getLogger('RunnerLogger')
    logger.setLevel(log_level.upper())

    #--- add formatting:
    log_format = '%(asctime)s : %(name)-12s : %(levelname)-8s : %(message)s'
    date_format = '%Y-%m-%d %H.%M.%S'
    formatter = logging.Formatter(log_format, date_format)

    #--- create rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000, backupCount=20)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    #--- create console handler:
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    #--- ensure any uncaught exceptions get logged:
    sys.excepthook = excepthook

    return logger


def excepthook(*args):
    runner_logger.critical('uncaught exception - exiting now', exc_info=args)
    sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
