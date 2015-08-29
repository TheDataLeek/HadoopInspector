#!/usr/bin/env python3

import sys
import os
import argparse
import logging
import collections
import subprocess
import json
from pprint import pprint as pp
from os.path import basename, isdir, isfile, exists
from os.path import join as pjoin

import hadoopinspector.core as core



def main():
    args       = get_args()
    configure_logger(args.log_dir)

    registry   = core.Registry()
    registry.load_registry(args.registry_filename,
                           args.instance, args.database, args.table, args.check)
    check_repo = CheckRepo(args.check_dir)
    checker    = CheckRunner(registry, check_repo)
    checker.run_checks_for_tables(args.table)

    if args.report:
        for rec in checker.results.get_formatted_results():
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
    parser.add_argument('-r', '--report',
                        action='store_true',
                        default=False,
                        help='indicates that a report should be generated')
    parser.add_argument('--registry-filename',
                        help='registry file contains check config')
    parser.add_argument('--check-dir',
                        help='which directory to look for the tests in')
    parser.add_argument('--log-dir',
                        help='specifies directory to log output to')

    args = parser.parse_args()

    if not args.check_dir and not isdir(args.check_dir):
        print('Supplied test directory does not exist. Please create.')
        sys.exit(1)
    if not isdir(args.log_dir):
        print('Supplied log directory does not exist.  Please create.')
        sys.exit(1)
    if not isfile(args.registry_filename):
        print('Supplied registry-filename does not exist.  Please correct.')
        sys.exit(1)

    return args



class CheckRunner(object):
    def __init__(self, registry, check_repo):
        """ This is the general test runner class

        :param check_repo: CheckRepository object
        """
        self.repo     = check_repo
        self.registry = registry
        self.results  = CheckResults()


    def run_checks_for_tables(self, table):
        """
        Runs checks on all tables

        :return: None
        """
        for repo_table in self.repo.tables:
            if table and table != repo_table:
                continue
            for check in self.repo.repo[repo_table]:
                count, rc = self._run_check(repo_table, check)
                self.results.add(repo_table, check, count, rc)

    def _run_check(self, table, check):
        check_fn = self.repo.repo[table][check]['fqfn']
        raw_output, rc = self._run_check_file(check_fn)
        #json_output = json.loads(raw_output)
        for rec in raw_output:
            if 'violation_cnt:' in rec:
                fields = rec.split()
                count = int(fields[1])
        return count, rc

    def _run_check_file(self, check_filename):
        process = subprocess.Popen([check_filename], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.read()
        rc     = process.returncode
        return output.decode().split('\n'), rc



class CheckRepo(object):

    def __init__(self, check_dir, instance, database):
        self.check_dir = check_dir
        # check repo is constructed here
        # check_repo contains a dictionary of table names
        # each table name contains a dictionary of checks
        # each cehck contains a dictionary of info about that check and how to # run it
        # todo: expand on this description
        self.repo      = {}
        for table in os.listdir(self.check_dir):
            self.repo[table] = self._get_table_checks(table)
        self.tables = list(self.repo.keys())

    def _get_table_checks(self, table):
        """
        Gets check info for all checks within a table's directory.

        :param table: str
            Name of table
        :return: dict(dict)
            A dictionary containing checks, with each check being its own
            dictinoary of check attributes
        """
        table_checks = {}
        for filename in os.listdir(pjoin(self.check_dir, table)):
            table_checks[filename] = {}
            table_checks[filename]['fqfn']       = pjoin(self.check_dir, table, filename)
            table_checks[filename]['check_type'] = 'rule' if 'rule'in filename else 'profiler'
        return table_checks



class CheckResults(object):

    def __init__(self):
        self.results = collections.defaultdict(dict)

    def add(self, table, check, violations, rc):
        if check not in self.results[table]:
            self.results[table][check] = {}
        self.results[table][check]['violation_cnt'] = violations
        self.results[table][check]['rc'] = rc

    def get_max_rc(self):
        max_rc = 0
        for table in self.results:
            for check in self.results[table]:
                if self.results[table][check]['rc'] > max_rc:
                    max_rc = self.results[table][check]['rc']
        return max_rc

    def get_tables(self):
        return list(self.results.keys())

    def get_formatted_results(self):
        formatted_results = []
        for table in self.results:
             for check in self.results[table]:
                 rec = '%s|%s|%s|%s' % (table, check,
                                        self.results[table][check]['rc'],
                                        self.results[table][check]['violation_cnt'])
                 formatted_results.append(rec)
        return formatted_results



def configure_logger(logdir):
    assert isdir(logdir)
    logfile = pjoin(logdir, 'runner.log')
    logging.basicConfig(filename=logfile,
                        level=logging.DEBUG)


if __name__ == '__main__':
    sys.exit(main())
