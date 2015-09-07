#!/usr/bin/env python3

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
    check_results  = core.CheckResults()

    checker    = CheckRunner(registry, check_repo, check_results)
    checker.store_variable_to_env('hapinsp_instance', args.instance)
    checker.store_variable_to_env('hapinsp_database', args.database)
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
    parser.add_argument('--check',
                        required=False,
                        help='specifies a single check to test against')
    parser.add_argument('-r', '--report',
                        action='store_true',
                        default=False,
                        help='indicates that a report should be generated')
    parser.add_argument('--registry-filename',
                        required=True,
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

    def __init__(self, registry, check_repo, check_results):
        """ This is the general test runner class

        :param check_repo: CheckRepository object
        """
        self.repo     = check_repo
        self.registry = registry
        self.results  = check_results

    def store_variable_to_env(self, key, value):
        os.putenv(key, value)


    def run_checks_for_tables(self, table):
        """
        Runs checks on all tables

        :return: None
        """
        for instance in self.registry.db_registry:
            for database in self.registry.db_registry[instance]:
                for table in self.registry.db_registry[instance][database]:
                    self.store_variable_to_env('hapinsp_table', table)
                    for check in self.registry.db_registry[instance][database][table]:
                        reg_check = self.registry.db_registry[instance][database][table][check]
                        if reg_check['check_status'] == 'active':
                            count, rc = self._run_check(reg_check)
                            check_status = 'active'
                        else:
                            count = None
                            rc    = None
                            check_status = reg_check['check_status']
                        self.results.add(instance, database, table, check, count, rc, check_status)

    def _run_check(self, reg_check):
        check_fn       = self.repo.repo[reg_check['check_name']]['fqfn']
        raw_output, rc = self._run_check_file(check_fn)
        #json_output = json.loads(raw_output)
        violation_cnt  = self.parse_check_results(raw_output)
        return violation_cnt, rc

    def parse_check_results(self, raw_output):
        check = None
        for rec in raw_output:
            if 'violation_cnt:' in rec:
                fields = rec.split()
                count = int(fields[1])
                assert core.isnumeric(count)
        return count

    def _run_check_file(self, check_filename):
        assert isdir(self.repo.check_dir)
        assert isfile(pjoin(self.repo.check_dir, check_filename))
        check_fqfn = pjoin(self.repo.check_dir, check_filename)
        process = subprocess.Popen([check_fqfn], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.read()
        rc     = process.returncode
        return output.decode().split('\n'), rc





def configure_logger(logdir):
    assert isdir(logdir)
    logfile = pjoin(logdir, 'runner.log')
    logging.basicConfig(filename=logfile,
                        level=logging.DEBUG)


if __name__ == '__main__':
    sys.exit(main())
