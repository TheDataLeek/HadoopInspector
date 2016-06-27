#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""

from __future__ import division
import sys, os, shutil
import logging
import tempfile, json
from pprint import pprint as pp
from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname
import pytest

sys.path.insert(0, dirname(dirname(dirname(os.path.abspath(__file__)))))
sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))

import hadoopinspector.registry as mod

logging.basicConfig()


class TestRegistry(object):

    def setup_method(self, method):
        self.temp_dir = tempfile.mkdtemp(prefix='hadinsp_')

    def teardown_method(self, method):
        shutil.rmtree(self.temp_dir)

    def test_loading_good_registry(self):
        good_data =    { "asset": {
				"rule_pk1": {
				"check_type":   "rule",
				"check_name":   "rule_uniqueness",
				"check_mode":   "full",
				"check_scope":  "row",
				"check_status": "active",
				"hapinsp_checkcustom_cols": 999
				}
			},
			"cust": {
				"rule_pk1": {
				"check_type":   "rule",
				"check_name":   "rule_uniqueness",
				"check_mode":   "full",
				"check_scope":  "row",
				"check_status": "active",
				"hapinsp_checkcustom_cols": 999
				}
			}
                      }
        with open(pjoin(self.temp_dir, 'registry.json'), 'w') as f:
            json.dump(good_data, f)
        reg = mod.Registry()
        reg.load_registry(pjoin(self.temp_dir, 'registry.json'))
        #reg.validate_file(pjoin(self.temp_dir, 'registry.json'))
        reg.validate_file(pjoin(self.temp_dir))

    def test_loading_bad_registry_extra_comma(self):
        # extra comma before last field in registry
        bad_data = ("""{"asset": {"rule_pk1": {"check_type": "rule", """
                    """ "check_name": "rule_uniqueness", "check_mode": "full", "check_scope": "row", """
                    """ "hapinsp_checkcustom_cols": 999, "check_status": "active"}}, """
                    """ "cust": {"rule_pk1": {"check_type": "rule", "check_name": "rule_uniqueness", """
                    """ "check_mode": "full", "check_scope": "row", "hapinsp_checkcustom_cols": 999,, """
                    """ "check_status": "active"}}} """)

        with open(pjoin(self.temp_dir, 'registry.json'), 'w') as f:
            f.write(bad_data)
        reg = mod.Registry()
        with pytest.raises(SystemExit):
            reg.load_registry(pjoin(self.temp_dir, 'registry.json'))


    def test_loading_bad_registry_unquoted_key(self):
        # check_status at end of registry is unquoted
        bad_data = ("""{"asset": {"rule_pk1": {"check_type": "rule", """
                    """ "check_name": "rule_uniqueness", "check_mode": "full", "check_scope": "row", """
                    """ "hapinsp_checkcustom_cols": 999, "check_status": "active"}}, """
                    """ "cust": {"rule_pk1": {"check_type": "rule", "check_name": "rule_uniqueness", """
                    """ "check_mode": "full", "check_scope": "row", "hapinsp_checkcustom_cols": 999, """
                    """ check_status: "active"}}} """)

        with open(pjoin(self.temp_dir, 'registry.json'), 'w') as f:
            f.write(bad_data)
        reg = mod.Registry()
        with pytest.raises(SystemExit):
            reg.load_registry(pjoin(self.temp_dir, 'registry.json'))


    def test_loading_bad_registry_missing_brace(self):
        # check_status at end of registry is unquoted
        bad_data = ("""{"asset": {"rule_pk1": {"check_type": "rule", """
                    """ "check_name": "rule_uniqueness", "check_mode": "full", "check_scope": "row", """
                    """ "hapinsp_checkcustom_cols": 999, "check_status": "active"}}, """
                    """ "cust": {"rule_pk1": {"check_type": "rule", "check_name": "rule_uniqueness", """
                    """ "check_mode": "full", "check_scope": "row", "hapinsp_checkcustom_cols": 999, """
                    """ "check_status": "active"}} """)

        with open(pjoin(self.temp_dir, 'registry.json'), 'w') as f:
            f.write(bad_data)
        reg = mod.Registry()
        with pytest.raises(SystemExit):
            reg.load_registry(pjoin(self.temp_dir, 'registry.json'))

    def test_creating_then_writing(self):
        reg = mod.Registry()
        reg.add_table('asset')
        reg.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row')
        reg.write(pjoin(self.temp_dir, 'registry.json'))
        reg.validate_file(pjoin(self.temp_dir, 'registry.json'))
        #with open(pjoin(self.temp_dir, 'registry.json')) as f:
        #    pp(f.read())

    def test_creating_then_loading(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row')
        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

        reg2 = mod.Registry()
        reg2.load_registry(pjoin(self.temp_dir, 'registry.json'))

        assert reg1.registry == reg2.registry

    def test_validating_bad_check(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='fullish',  # this is bad!
               check_scope='row')
        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        with pytest.raises(SystemExit):
            reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

    def test_validating_bad_setup(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='setup',
               check_mode='full',  # this is bad! should be None
               check_scope='row')  # this is bad! should be None
        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        with pytest.raises(SystemExit):
            reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

    def test_creating_then_loading_with_setup_and_teardown(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'asset_setup',
               check_name='asset_setup', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)

        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')

        reg1.add_check('asset', 'teardown',
               check_name='asset_teardown', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)

        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

        reg2 = mod.Registry()
        reg2.load_registry(pjoin(self.temp_dir, 'registry.json'))

        assert reg1.registry == reg2.registry

    def test_filter_registry_no_args(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'asset_setup',
               check_name='asset_setup', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.add_check('asset', 'teardown',
               check_name='asset_teardown', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))
        reg1.filter_registry()
        assert 'check_name' in reg1.registry['asset']['rule_pk1']

    def test_filter_registry_with_table(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.add_table('cust')
        reg1.add_check('cust', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))
        reg1.filter_registry('cust')
        assert 'cust' in reg1.registry
        assert 'rule_pk1' in reg1.registry['cust']
        assert 'asset' not in reg1.registry

    def test_filter_registry_with_check(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'asset_setup',
               check_name='asset_setup', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.add_check('asset', 'rule_fk1',
               check_name='rule_foreign_key', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.add_check('asset', 'teardown',
               check_name='asset_teardown', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)
        reg1.add_table('cust')
        reg1.add_check('cust', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))
        reg1.filter_registry(None, 'rule_pk1')
        assert 'cust' in reg1.registry
        assert 'rule_pk1' in reg1.registry['cust']
        assert 'asset' in reg1.registry
        assert 'rule_pk1' in reg1.registry['asset']
        assert 'rule_fk1' not in reg1.registry['asset']

    def test_filter_registry_with_table_and_check(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'asset_setup',
               check_name='asset_setup', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.add_check('asset', 'rule_fk1',
               check_name='rule_foreign_key', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')
        reg1.add_check('asset', 'teardown',
               check_name='asset_teardown', check_status='active',
               check_type='setup', check_mode=None, check_scope=None)
        reg1.add_table('cust')
        reg1.add_check('cust', 'rule_pk1',
               check_name='rule_uniqueness', check_status='active',
               check_type='rule', check_mode='full', check_scope='row')

        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))
        reg1.filter_registry('cust', 'rule_pk1')
        assert 'cust' in reg1.registry
        assert 'rule_pk1' in reg1.registry['cust']
        assert 'asset' not in reg1.registry

    def test_validation(self):
        reg1 = mod.Registry()
        with open(pjoin(self.temp_dir, 'registry.json'), 'w') as f:
            f.write('im an invalid data structure')

        with pytest.raises(SystemExit):
            reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

    def test_validating_checkvars(self):
        reg1 = mod.Registry()
        reg1.add_table('asset')
        reg1.add_check('asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='setup',
               check_mode=None,  # this is bad! should be None
               check_scope=None,  # this is bad! should be None
               hapinsp_checkcustom_foo='bar')
        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

