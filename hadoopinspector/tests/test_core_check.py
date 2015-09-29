#!/usr/bin/env python34
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""

from __future__ import division
import sys, os, shutil, stat
import tempfile, json
import subprocess
import collections
from pprint import pprint as pp
import pytest

from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import core as mod


class TestSetupVars(object):

    def setup_method(self, method):
        self.registry      = None
        self.check_repo    = None
        self.check_results = None
        self.check_runner  = mod.CheckRunner(self.registry, self.check_repo,
                              self.check_results, instance='foo', database='bar')

    def teardown_method(self, method):
        pass

    def test_parse_setup_check_results__with_good_data(self):
        kv = {"hapinsp_tablecustom_year":2015,
              "hapinsp_tablecustom_month": 4,
              "hapinsp_tablecustom_day": 30}
        raw_output = """{"hapinsp_tablecustom_year":2015,
                         "hapinsp_tablecustom_month": 4,
                         "hapinsp_tablecustom_day": 30} """
        setup_vars = mod.SetupVars(raw_output)
        assert setup_vars.tablecustom_vars == kv

    def test_parse_setup_check_results__with_no_data(self):
        kv = {}
        raw_output = """{}"""
        setup_vars = mod.SetupVars(raw_output)
        assert setup_vars.tablecustom_vars == kv

    def test_parse_setup_check_results__with_None_data(self):
        with pytest.raises(TypeError):
           setup_vars = mod.SetupVars(None)

    def test_parse_setup_check_results__with_corrupt_struct(self):
        raw_output = """{"""
        with pytest.raises(ValueError):
           setup_vars = mod.SetupVars("""{""")

    def test_parse_setup_check_results__with_bad_key(self):
        raw_output = """{"hapinsp_tableblah_foo": 2015 } """
        with pytest.raises(ValueError):
            setup_vars = mod.SetupVars(raw_output)



class TestVars(object):

    def setup_method(self, method):
        self.registry      = None
        self.check_repo    = None
        self.check_results = None
        self.check_runner  = mod.CheckRunner(self.registry, self.check_repo,
                              self.check_results, instance='foo', database='bar')

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




















