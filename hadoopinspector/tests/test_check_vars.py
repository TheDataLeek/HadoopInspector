#!/usr/bin/env python34
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""
from __future__ import division
import sys, os, shutil, errno
import tempfile
import logging
import logging.handlers
from pprint import pprint as pp
import pytest

from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import check_runner as mod


class TestSetupVars(object):

    def setup_method(self, method):
        self.registry      = None
        self.instance      = 'foo'
        self.database      = 'bar'
        self.check_repo    = None
        self.check_results = None
        self.log_dir       = tempfile.mkdtemp(prefix='hapinsp_run_logs_')
        self.check_runner  = mod.CheckRunner(self.registry, self.check_repo,
                              self.check_results, instance='foo', database='bar', run_log_dir=self.log_dir)
        self._get_logger(table='tab1', check='ck1')
        self.logger.debug('setup complete')

    def _get_logger(self, table, check):

        def mkdirs(path):
            try:
                os.makedirs(path)
            except OSError as exc:
                if exc.errno != errno.EEXIST or not os.path.isdir(path):
                    raise

        assert isdir(self.log_dir)
        check_log_dir = pjoin(self.log_dir, self.instance, self.database, table, check)
        mkdirs(check_log_dir)
        log_filename = pjoin(check_log_dir, 'check.log')

        #--- create logger
        self.logger = logging.getLogger('CheckLogger')
        self.logger.setLevel('DEBUG')

        #--- add formatting:
        log_format = '%(asctime)s : %(name)-12s : %(levelname)-8s : %(message)s'
        date_format = '%Y-%m-%d %H.%M.%S'
        formatter = logging.Formatter(log_format, date_format)

        #--- create rotating file handler
        self.file_handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000, backupCount=20)
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)

    def teardown_method(self, method):
        shutil.rmtree(self.log_dir)

    def test_parse_setup_check_results__with_good_data(self):
        kv = {"hapinsp_tablecustom_year":2015,
              "hapinsp_tablecustom_month": 4,
              "hapinsp_tablecustom_day": 30}
        raw_output = """{"hapinsp_tablecustom_year":2015,
                         "hapinsp_tablecustom_month": 4,
                         "hapinsp_tablecustom_day": 30} """
        setup_vars = mod.SetupVars(raw_output, self.logger)
        assert setup_vars.tablecustom_vars == kv

    def test_parse_setup_check_results__with_no_data(self):
        kv = {}
        raw_output = """{}"""
        setup_vars = mod.SetupVars(raw_output, self.logger)
        assert setup_vars.tablecustom_vars == kv

    def test_parse_setup_check_results__with_None_data(self):
        with pytest.raises(ValueError):
            mod.SetupVars(None, self.logger)

    def test_parse_setup_check_results__with_corrupt_struct(self):
        with pytest.raises(ValueError):
            mod.SetupVars("""{""", self.logger)

    def test_parse_setup_check_results__with_bad_key(self):
        raw_output = """{"hapinsp_tableblah_foo": 2015 } """
        with pytest.raises(ValueError):
            mod.SetupVars(raw_output, self.logger)



class TestVars(object):

    def setup_method(self, method):
        self.registry      = None
        self.check_repo    = None
        self.check_results = None
        self.log_dir       = tempfile.mkdtemp(prefix='hapinsp_run_log_')
        self.check_runner  = mod.CheckRunner(self.registry, self.check_repo,
                              self.check_results, instance='foo', database='bar', run_log_dir=self.log_dir)

    def teardown_method(self, method):
        pass

    def test_table_var_add(self):
        self.check_runner.add_table_var("hapinsp_tablecustom_foo", "bar")
        assert 'bar' == os.environ['hapinsp_tablecustom_foo']

    def test_table_var_drop(self):
        self.check_runner.add_table_var("hapinsp_tablecustom_foo", "bar")
        assert 'bar' == os.environ['hapinsp_tablecustom_foo']

        self.check_runner.drop_table_vars()
        with pytest.raises(KeyError):
            os.environ['hapinsp_tablecustom_foo']

    def test_table_var_duplicate_keys(self):
        self.check_runner.add_table_var("hapinsp_tablecustom_foo", "bar")
        assert 'bar' == os.environ['hapinsp_tablecustom_foo']

        self.check_runner.add_table_var("hapinsp_tablecustom_foo", "baz")
        assert 'baz' == os.environ['hapinsp_tablecustom_foo']

        self.check_runner.drop_table_vars()
        with pytest.raises(KeyError):
            os.environ['hapinsp_tablecustom_foo']


    def test_db_var_add(self):
        self.check_runner.add_db_var("hapinsp_database", "bar")
        assert 'bar' == os.environ['hapinsp_database']

    def test_db_var_drop(self):
        self.check_runner.add_db_var("hapinsp_database", "bar")
        assert 'bar' == os.environ['hapinsp_database']

        self.check_runner.drop_db_vars()
        with pytest.raises(KeyError):
            os.environ['hapinsp_database']

    def test_db_var_duplicates(self):
        self.check_runner.add_db_var("hapinsp_database", "bar")
        assert 'bar' == os.environ['hapinsp_database']
        self.check_runner.add_db_var("hapinsp_database", "baz")
        assert 'baz' == os.environ['hapinsp_database']

        self.check_runner.drop_db_vars()
        with pytest.raises(KeyError):
            os.environ['hapinsp_database']


    def test_priortable_var_add(self):
        self.check_runner.add_prior_table_var("hapinsp_tablecustom_foo", "bar")
        assert 'bar' == os.environ['hapinsp_tablecustom_foo_prior']

    def test_prior_table_var_drop(self):
        self.check_runner.add_prior_table_var("hapinsp_tablecustom_foo", "bar")
        assert 'bar' == os.environ['hapinsp_tablecustom_foo_prior']

        self.check_runner.drop_prior_table_vars()
        with pytest.raises(KeyError):
            os.environ['hapinsp_tablecustom_foo_prior']

    def test_prior_table_duplicates(self):
        self.check_runner.add_prior_table_var("hapinsp_tablecustom_foo", "bar")
        assert 'bar' == os.environ['hapinsp_tablecustom_foo_prior']
        self.check_runner.add_prior_table_var("hapinsp_tablecustom_foo", "baz")
        assert 'baz' == os.environ['hapinsp_tablecustom_foo_prior']

        self.check_runner.drop_prior_table_vars()
        with pytest.raises(KeyError):
            os.environ['hapinsp_tablecustom_foo_prior']




















