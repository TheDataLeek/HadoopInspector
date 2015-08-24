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


def main():
    args       = get_args()
    configure_logger(args.log_dir)

    check_repo = CheckRepo(args.check_dir)
    tester     = TestRunner(check_repo)
    tester.run_checks_for_all_tables()

    if args.report:
        for rec in tester.results.get_formatted_results():
            print(rec)

    sys.exit(tester.results.get_max_rc())




def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check-dir',
                        help='Which directory to look for the tests in')
    parser.add_argument('--log-dir',
                        help='specifies directory to log output to')
    parser.add_argument('-r', '--report',
                        action='store_true',
                        default=False,
                        help='indicates that a report should be generated')
    args = parser.parse_args()
    if not args.check_dir and not isdir(args.check_dir):
        print('Supplied test directory does not exist. Setting to default')
        args.testdir = './tests'
    return args



class TestRunner(object):
    def __init__(self, check_repo):
        """ This is the general test runner class

        :param check_repo: CheckRepository object
        """
        self.repo     = check_repo
        self.results  = CheckResults()


    def run_checks_for_all_tables(self):
        """
        Runs checks on all tables

        :return: None
        """
        for table in self.repo.tables:
            for check in self.repo.repo[table]:
                 count, rc = self._run_check(table, check)
                 self.results.add(table, check, count, rc)

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

    def __init__(self, check_dir):
        self.check_dir = check_dir
        # check repo is constructed here
        # check_repo contains a dictionary of table names
        # each table name contains a dictionary of checks
        # each cehck contains a dictionary of info about that check and how to # run it
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
