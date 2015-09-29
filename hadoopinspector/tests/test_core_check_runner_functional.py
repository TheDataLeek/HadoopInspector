#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""

from __future__ import division
import sys, os, shutil, stat, time
import tempfile
import subprocess
import collections
import sqlite3
from pprint import pprint as pp

from os.path import exists, isdir, isfile, basename, dirname
from os.path import join as pjoin
from os.path import dirname

script_path = os.path.dirname(os.path.dirname(os.path.realpath((__file__))))
pgm         = os.path.join(script_path, 'hadoopinspector_runner.py')

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(os.path.abspath(__file__)))))
import core as core
import tests.test_tooling  as test_tooling

Record = collections.namedtuple('Record', 'instance db table check check_rc violation_cnt')


class TestWithMockedCheckFiles(object):

    def setup_method(self, method):
        self.log_dir   = tempfile.mkdtemp(prefix='hadinsp_l_')
        self.check_dir = tempfile.mkdtemp(prefix='hadinsp_c_')
        self.misc_dir  = tempfile.mkdtemp(prefix='hadinsp_m_')
        self.registry_fqfn = None
        self.results_fqfn = pjoin(self.misc_dir, 'results.sqlite')
        self.inst      = 'inst1'
        self.db        = 'db1'
        self.hapinsp_formatter_fqfn = get_formatter_fqfn()

    def teardown_method(self, method):
        shutil.rmtree(self.check_dir)
        shutil.rmtree(self.log_dir)
        shutil.rmtree(self.misc_dir)

    def run_checker(self, table):
        registry = core.Registry()
        registry.load_registry(self.registry_fqfn)
        registry.generate_db_registry(self.inst, self.db)
        check_repo = core.CheckRepo(self.check_dir)
        check_results = core.CheckResults(db_fqfn=self.results_fqfn)

        checker = core.CheckRunner(registry, check_repo, check_results, self.inst, self.db)
        checker.add_db_var('hapinsp_instance', self.inst)
        checker.add_db_var('hapinsp_database', self.db)
        checker.run_checks_for_tables(table)
        report = []
        for line in checker.results.get_formatted_results():
            try:
                rec = test_tooling.report_rec_parser(line)
                report.append(rec)
            except:
                pass





    def _add_setup_check(self, table, key=None, value=None,
                         required_key=None, required_value=None, fqfn=None):
        fqfn = testtooling.add_setup_check(self.check_dir, key, value,
                                           required_key, required_value,
                                           fqfn,
                                           self.hapinsp_formatter_fqfn)
        self._add_to_registry(table, fqfn)
        return fqfn

    def _add_rule_check(self, table, return_rc, echo_count, register=True):
        fqfn = testtooling.add_check(self.check_dir,
                         table,
                         rc=return_rc,
                         out_count=echo_count,
                         formatter_fqfn=self.hapinsp_formatter_fqfn)
        if register:
            self._add_to_registry(table, fqfn)
        return fqfn

    def _add_env_rule_check(self, table, key, value):
        fqfn = testtooling.add_env_check(self.check_dir, table,
                             key, value,
                             formatter_fqfn=self.hapinsp_formatter_fqfn)
        self._add_to_registry(table, fqfn)
        return fqfn


    def _add_to_registry(self, table, check_fqfn):
        if basename(check_fqfn).startswith('setup'):
            check_type = 'setup'
        else:
            check_type = 'rule'
        self.registry_fqfn = testtooling.add_to_registry(self.registry_fqfn,
                                             self.inst,
                                             self.db,
                                             table,
                                             self.check_dir,
                                             check_fqfn,
                                             check_type)



    def notest_one_successful_check_with_no_violations(self):
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        self._add_rule_check(table, expected_check_rc, expected_violation_cnt)
        report, run_rc = self.run_checker()
        print("Report: ")
        pp(report)
        testtooling.report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def notest_one_successful_checks_with_violations(self):
        table                   = 'customer'
        expected_check_cnt      = 1
        expected_check_rc       = '0'
        expected_run_rc         = '0'
        expected_violation_cnt  = '9'
        self._add_rule_check(table, expected_check_rc, expected_violation_cnt)
        report, run_rc = self.run_cmd()
        testtooling.report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def notest_check_env_variable_table(self):
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        self._add_env_rule_check(table, key='hapinsp_table', value='customer')
        report, run_rc = self.run_cmd()
        testtooling.report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def notest_setup_then_check(self):
        table                  = 'customer'
        expected_check_cnt     = 2
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        self._add_setup_check(table, key='hapinsp_tablecustom_foo', value='bar')
        self._add_env_rule_check(table, key='hapinsp_tablecustom_foo', value='bar')
        report, run_rc = self.run_cmd()
        testtooling.report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc



    def notest_get_prior_setup(self):
        """
        """
        table                  = 'customer'
        print("")
        print("run #1 - everything should work, defines first hapinsp_tablecustom_foo value of 'inactive'")
        setup_fqfn = self._add_setup_check(table, key='hapinsp_tablecustom_foo', value='inactive')
        check_fqfn = self._add_env_rule_check(table, key='hapinsp_tablecustom_foo', value='inactive')
        report, run_rc = self.run_cmd()
        testtooling.report_checker(report, expected_check_cnt=2, expected_check_rc=0, expected_violation_cnt=0)
        time.sleep(1)

        print("")
        print("run #2 - change setup & recheck")
        self._add_setup_check(table, key='hapinsp_tablecustom_foo', value='active', fqfn=setup_fqfn)
        report, run_rc = self.run_cmd()
        testtooling.report_checker(report, expected_check_cnt=2, expected_check_rc=0, expected_violation_cnt=1)
        time.sleep(1)

        #---------- Run #3 --------------
        print("")
        print("run #3 - must find prior hapinsp_tablecustom_foo value of 'active'")
        # override setup to require prior value:
        self._add_setup_check(table, required_key='hapinsp_tablecustom_foo_prior', required_value='active', fqfn=setup_fqfn)
        report, run_rc = self.run_cmd()
        testtooling.report_checker(report, expected_check_cnt=2, expected_check_rc=0, expected_violation_cnt=1)






class EmptyRecError(Exception):
    def __init__(self, value=None):
        self.value = value
    def __str__(self):
        return repr(self.value)


def print_file(filename):
    with open(filename, 'r') as f:
         print(f.read())






