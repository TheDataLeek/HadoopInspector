#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""

from __future__ import division
import sys, os, shutil, stat
import logging, datetime, tempfile
from datetime import datetime as dtdt
from pprint import pprint as pp
from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname
import sqlite3

sys.path.insert(0, dirname(dirname(dirname(os.path.abspath(__file__)))))
sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))

import hadoopinspector.check_results as check_results
import hadoopinspector.core as core

logging.basicConfig()



class TestCheckRepo(object):

    def setup_method(self, method):
        self.check_dir = tempfile.mkdtemp(prefix='hadinsp_ckdir_')

    def teardown_method(self, method):
        shutil.rmtree(self.check_dir)

    def test_get_table_checks(self):
        add_check(self.check_dir)
        add_check(self.check_dir)
        add_check(self.check_dir)
        self.repo = core.CheckRepo(self.check_dir)

        assert len(self.repo.repo) == 3
        for check in self.repo.repo:
            assert isfile(self.repo.repo[check]['fqfn'])



class TestCheckResults(object):

    def setup_method(self, method):
        self.inst  = 'inst1'
        self.db    = 'db2'
        self.temp_dir  = tempfile.mkdtemp(prefix="hadinsp_")
        self.fqfn  = pjoin(self.temp_dir, 'results.sqlite')
        self.check_results = check_results.CheckResults(self.inst, self.db, self.fqfn)

    def teardown_method(self, method):
        shutil.rmtree(self.temp_dir)

    def add_2_checks_to_1_table(self, table, violations=0, rc=0):
        start_dt = dtdt.utcnow() - datetime.timedelta(minutes=1)
        stop_dt  = dtdt.utcnow()
        self.check_results.add(table, 'check_fk1', violations, rc,
                               run_start_timestamp=start_dt,
                               run_stop_timestamp=stop_dt)
        self.check_results.add(table, 'check_fk2', violations, rc,
                               run_start_timestamp=start_dt,
                               run_stop_timestamp=stop_dt)

    def add_1_demo_check_to_1_table(self, table, violations, rc, start_dt, stop_dt):
        self.check_results.add(table, 'check_fk1', violations, rc,
                               run_start_timestamp=start_dt,
                               run_stop_timestamp=stop_dt)

    def test_add(self):
        self.add_2_checks_to_1_table('customer', 3, 0)
        results = self.check_results.results['customer']
        assert results['check_fk1']['rc'] == 0
        assert results['check_fk1']['violation_cnt'] == 3
        assert results['check_fk2']['rc'] == 0
        assert results['check_fk2']['violation_cnt'] == 3

    def test_add_str(self):
        self.add_2_checks_to_1_table('customer', '3', '0')
        results = self.check_results.results['customer']
        assert results['check_fk1']['rc'] == 0
        assert results['check_fk1']['violation_cnt'] == '3'
        assert results['check_fk2']['rc'] == 0
        assert results['check_fk2']['violation_cnt'] == '3'

    def test_demo_add(self):
        start_dt = datetime.datetime(2015, 1, 2, 15, 22, 59)
        stop_dt  = datetime.datetime(2015, 1, 2, 15, 23, 59)
        self.add_1_demo_check_to_1_table('customer', '3', '0', start_dt, stop_dt)
        results = self.check_results.results['customer']
        assert results['check_fk1']['rc'] == 0
        assert results['check_fk1']['violation_cnt'] == '3'
        assert results['check_fk1']['run_start_timestamp'] == start_dt
        assert results['check_fk1']['run_stop_timestamp']  == stop_dt

    def test_get_tables(self):
        self.add_2_checks_to_1_table('customer')
        self.check_results.results.keys() == ['customer']

    def test_max_results(self):
        self.add_2_checks_to_1_table('customer')
        self.add_2_checks_to_1_table('asset', rc=4)
        assert self.check_results.get_max_rc() == 4

    def test_creating_sqlite_table(self):
        self.add_2_checks_to_1_table('customer', violations=3)
        self.check_results.write_to_sqlite()
        assert isfile(pjoin(self.temp_dir, 'results.sqlite'))

        conn = sqlite3.connect(pjoin(self.temp_dir, 'results.sqlite'))
        cur  = conn.cursor()
        sql  = "SELECT * FROM check_results"
        cur.execute(sql)
        results = cur.fetchall()
        assert len(results)    == 2
        assert len(results[0]) == 19
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


