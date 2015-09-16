#!/usr/bin/env python34
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


class TestCheckRunner(object):

    def setup_method(self, method):
        self.registry      = None
        self.check_repo    = None
        self.check_results = None
        self.check_runner  = mod.CheckRunner(self.registry, self.check_repo, self.check_results)

    def teardown_method(self, method):
        pass

    def test_parse_setup_check_results__with_good_data(self):
        kv = {"hapinsp_tablevar_year":2015,
              "hapinsp_tablevar_month": 4,
              "hapinsp_tablevar_day": 30}
        raw_output = """{"hapinsp_tablevar_year":2015,
                         "hapinsp_tablevar_month": 4,
                         "hapinsp_tablevar_day": 30} """
        assert self.check_runner.parse_setup_check_results(raw_output) == kv
        assert self.check_runner.parse_setup_check_results(raw_output) == kv

    def test_parse_setup_check_results__with_no_data(self):
        kv = {}
        raw_output = """{}"""
        assert self.check_runner.parse_setup_check_results(raw_output) == kv

    def test_parse_setup_check_results__with_None_data(self):
        with pytest.raises(TypeError):
            self.check_runner.parse_setup_check_results(None)

    def test_parse_setup_check_results__with_corrupt_struct(self):
        raw_output = """{"""
        with pytest.raises(ValueError):
            self.check_runner.parse_setup_check_results(raw_output)

    def test_parse_setup_check_results__with_bad_key(self):
        raw_output = """{"hapinsp_tableblah_foo": 2015 } """
        with pytest.raises(ValueError):
            self.check_runner.parse_setup_check_results(raw_output)















