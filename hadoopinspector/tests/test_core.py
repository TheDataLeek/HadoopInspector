#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""

from __future__ import division
import sys, os, shutil, stat
import time, datetime
import tempfile, json
import subprocess
import copy
import collections
from pprint import pprint as pp
from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname
import pytest
import sqlite3

script_path = os.path.dirname(os.path.dirname(os.path.realpath((__file__))))
pgm         = os.path.join(script_path, 'hadoopinspector_runner.py')

sys.path.insert(0, dirname(dirname(dirname(os.path.abspath(__file__)))))
sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))

import hadoopinspector.core as mod

Record = collections.namedtuple('Record', 'table check check_rc violation_cnt')



class TestRegistry(object):

    def setup_method(self, method):
        self.temp_dir = tempfile.mkdtemp(prefix='hadinsp_')

    def teardown_method(self, method):
        shutil.rmtree(self.temp_dir)


    def test_creating_then_writing(self):
        reg = mod.Registry()
        reg.add_instance('prod1')
        reg.add_db('prod1', 'AssetEvent')
        reg.add_table('prod1', 'AssetEvent', 'asset')
        reg.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
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
        reg1.add_instance('prod1')
        reg1.add_db('prod1', 'AssetEvent')
        reg1.add_table('prod1', 'AssetEvent', 'asset')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row')
        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

        reg2 = mod.Registry()
        reg2.load_registry(pjoin(self.temp_dir, 'registry.json'))

        assert reg1.full_registry == reg2.full_registry

    def test_validating_bad_check(self):
        reg1 = mod.Registry()
        reg1.add_instance('prod1')
        reg1.add_db('prod1', 'AssetEvent')
        reg1.add_table('prod1', 'AssetEvent', 'asset')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
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
        reg1.add_instance('prod1')
        reg1.add_db('prod1', 'AssetEvent')
        reg1.add_table('prod1', 'AssetEvent', 'asset')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
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
        reg1.add_instance('prod1')
        reg1.add_db('prod1', 'AssetEvent')
        reg1.add_table('prod1', 'AssetEvent', 'asset')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'asset_setup',
               check_name='asset_setup',
               check_status='active',
               check_type='setup',
               check_mode=None,
               check_scope=None)

        reg1.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row')

        reg1.add_check('prod1', 'AssetEvent', 'asset', 'teardown',
               check_name='asset_teardown',
               check_status='active',
               check_type='setup',
               check_mode=None,
               check_scope=None)

        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

        reg2 = mod.Registry()
        reg2.load_registry(pjoin(self.temp_dir, 'registry.json'))

        assert reg1.full_registry == reg2.full_registry

    def test_generating_db_registry(self):
        reg1 = mod.Registry()
        reg1.add_instance('prod1')
        reg1.add_db('prod1', 'AssetEvent')
        reg1.add_table('prod1', 'AssetEvent', 'asset')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'asset_setup',
               check_name='asset_setup',
               check_status='active',
               check_type='setup',
               check_mode=None,
               check_scope=None)
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'teardown',
               check_name='asset_teardown',
               check_status='active',
               check_type='setup',
               check_mode=None,
               check_scope=None)
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))
        reg1.generate_db_registry('prod1', 'AssetEvent')
        assert 'check_name' in reg1.db_registry['prod1']['AssetEvent']['asset']['rule_pk1']

    def test_validation(self):
        reg1 = mod.Registry()
        with open(pjoin(self.temp_dir, 'registry.json'), 'w') as f:
            f.write('im an invalid data structure')

        with pytest.raises(SystemExit):
            reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))

    def test_validating_checkvars(self):
        reg1 = mod.Registry()
        reg1.add_instance('prod1')
        reg1.add_db('prod1', 'AssetEvent')
        reg1.add_table('prod1', 'AssetEvent', 'asset')
        reg1.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='setup',
               check_mode=None,  # this is bad! should be None
               check_scope=None,  # this is bad! should be None
               hapinsp_checkcustom_foo='bar')
        reg1.write(pjoin(self.temp_dir, 'registry.json'))
        reg1.validate_file(pjoin(self.temp_dir, 'registry.json'))




class TestCheckRepo(object):

    def setup_method(self, method):
        self.check_dir = tempfile.mkdtemp(prefix='hadinsp_ckdir_')

    def teardown_method(self, method):
        shutil.rmtree(self.check_dir)

    def test_get_table_checks(self):
        add_check(self.check_dir)
        add_check(self.check_dir)
        add_check(self.check_dir)
        self.repo = mod.CheckRepo(self.check_dir)

        assert len(self.repo.repo) == 3
        for check in self.repo.repo:
            assert isfile(self.repo.repo[check]['fqfn'])



class TestCheckResults(object):

    def setup_method(self, method):
        self.inst  = 'inst1'
        self.db    = 'db2'
        self.temp_dir  = tempfile.mkdtemp(prefix="hadinsp_")
        self.fqfn  = pjoin(self.temp_dir, 'results.sqlite')
        self.check_results = mod.CheckResults(self.fqfn)

    def teardown_method(self, method):
        shutil.rmtree(self.temp_dir)

    def add_2_checks_to_1_table(self, table, violations=0, rc=0):
        self.check_results.add(self.inst, self.db, table, 'check_fk1', violations, rc)
        self.check_results.add(self.inst, self.db, table, 'check_fk2', violations, rc)

    def add_1_demo_check_to_1_table(self, table, violations, rc, start_dt, stop_dt):
        self.check_results.add(self.inst, self.db, table, 'check_fk1', violations, rc,
                               run_start_timestamp=start_dt,
                               run_stop_timestamp=stop_dt)

    def test_add(self):
        self.add_2_checks_to_1_table('customer', 3, 0)
        table_results = self.check_results.results[self.inst][self.db]['customer']
        assert table_results['check_fk1']['rc'] == 0
        assert table_results['check_fk1']['violation_cnt'] == 3
        assert table_results['check_fk2']['rc'] == 0
        assert table_results['check_fk2']['violation_cnt'] == 3

    def test_add_str(self):
        self.add_2_checks_to_1_table('customer', '3', '0')
        table_results = self.check_results.results[self.inst][self.db]['customer']
        assert table_results['check_fk1']['rc'] == 0
        assert table_results['check_fk1']['violation_cnt'] == '3'
        assert table_results['check_fk2']['rc'] == 0
        assert table_results['check_fk2']['violation_cnt'] == '3'

    def test_demo_add(self):
        start_dt = datetime.datetime(2015, 1, 2, 15, 22, 59)
        stop_dt  = datetime.datetime(2015, 1, 2, 15, 23, 59)
        self.add_1_demo_check_to_1_table('customer', '3', '0', start_dt, stop_dt)
        table_results = self.check_results.results[self.inst][self.db]['customer']
        assert table_results['check_fk1']['rc'] == 0
        assert table_results['check_fk1']['violation_cnt'] == '3'
        assert table_results['check_fk1']['run_start_timestamp'] == start_dt
        assert table_results['check_fk1']['run_stop_timestamp']  == stop_dt

    def test_get_tables(self):
        self.add_2_checks_to_1_table('customer')
        self.check_results.results[self.inst][self.db].keys() == ['customer']
        #assert self.check_results.get_tables() == ['customer']

    def test_max_results(self):
        self.add_2_checks_to_1_table('customer')
        self.add_2_checks_to_1_table('asset', rc=4)
        assert self.check_results.get_max_rc() == 4

    def test_creating_sqlite_table(self):
        self.add_2_checks_to_1_table('customer', violations=3)
        self.check_results.write_to_sqlite()
        assert isfile(pjoin(self.temp_dir, 'results.sqlite'))
        shutil.copy(pjoin(self.temp_dir, 'results.sqlite'), pjoin('/tmp', 'results.sqlite'))

        conn = sqlite3.connect(pjoin(self.temp_dir, 'results.sqlite'))
        cur  = conn.cursor()
        sql  = "SELECT * FROM check_results"
        cur.execute(sql)
        results = cur.fetchall()
        assert len(results)    == 2
        assert len(results[0]) == 17
        assert results[0][-2]  == 3   #check_violations_cnt
        assert results[1][-2]  == 3   #check_violations_cnt

        sql  = "SELECT max(run_start_timestamp), current_timestamp, \
                       max(run_start_timestamp) - current_timestamp  as time_diff\
                FROM check_results"
        cur.execute(sql)
        results = cur.fetchall()
        assert results[0][2] in (0, 1), "should run in 0 seconds normally, 1 second worst-case"
        conn.close()



def add_check(check_dir, rc=0, out_count=0):
    if not isdir(check_dir):
        os.mkdir(check_dir)

    for check_id in range(1000):
       fqfn = pjoin(check_dir, 'rule_%d.bash' % check_id)
       if not isfile(fqfn):
           break

    with open(fqfn, 'w') as f:
        f.write(u'#!/usr/bin/env bash \n')
        if out_count is not None:
            f.write(u'echo "violation_cnt: %s" \n' % out_count)
        f.write(u'exit %s \n' % rc)
    st = os.stat(fqfn)
    os.chmod(fqfn, st.st_mode | stat.S_IEXEC)


