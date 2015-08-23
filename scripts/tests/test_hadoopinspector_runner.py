#!/usr/bin/env python34
from __future__ import division
import sys, os, shutil, stat
import tempfile
import subprocess
import collections
from pprint import pprint as pp

from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname

script_path = os.path.dirname(os.path.dirname(os.path.realpath((__file__))))
pgm         = os.path.join(script_path, 'hadoopinspector_runner.py')

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))

import hadoopinspector_runner  as mod

Record = collections.namedtuple('Record', 'table check check_rc violation_cnt')


class TestCheckResults(object):

    def setup_method(self, method):
        self.check_results = mod.CheckResults()

    def teardown_method(self, method):
        pass

    def add_2_checks_to_1_table(self, table, violations=0, rc=0):
        self.check_results.add(table, 'check_fk1', violations, rc)
        self.check_results.add(table, 'check_fk2', violations, rc)

    def test_add(self):
        self.add_2_checks_to_1_table('customer', 3, 0)
        assert self.check_results.results['customer']['check_fk1']['rc'] == 0
        assert self.check_results.results['customer']['check_fk1']['violation_cnt'] == 3
        assert self.check_results.results['customer']['check_fk2']['rc'] == 0
        assert self.check_results.results['customer']['check_fk2']['violation_cnt'] == 3

    def test_add_str(self):
        self.add_2_checks_to_1_table('customer', '3', '0')
        assert self.check_results.results['customer']['check_fk1']['rc'] == '0'
        assert self.check_results.results['customer']['check_fk1']['violation_cnt'] == '3'
        assert self.check_results.results['customer']['check_fk2']['rc'] == '0'
        assert self.check_results.results['customer']['check_fk2']['violation_cnt'] == '3'

    def test_get_tables(self):
        self.add_2_checks_to_1_table('customer')
        assert self.check_results.get_tables() == ['customer']

    def test_max_results(self):
        self.add_2_checks_to_1_table('customer')
        self.add_2_checks_to_1_table('asset', rc=4)
        assert self.check_results.get_max_rc() == 4



class TestCheckRepo(object):

    def setup_method(self, method):
        self.check_dir = tempfile.mkdtemp(prefix='hadinsp_ckdir_')

    def teardown_method(self, method):
        shutil.rmtree(self.check_dir)

    def test_get_table_checks(self):
        add_check(self.check_dir, 'customer')
        add_check(self.check_dir, 'customer')
        add_check(self.check_dir, 'asset')
        self.repo = mod.CheckRepo(self.check_dir)

        assert sorted(self.repo.tables) == sorted(['customer', 'asset'])
        assert len(self.repo.repo['customer']) == 2
        for check in self.repo.repo['customer']:
            assert isfile(pjoin(self.check_dir, 'customer', check))
            assert self.repo.repo['customer'][check]['check_type'] == 'rule'
            assert isfile(self.repo.repo['customer'][check]['fqfn'])




def add_check(check_dir, table, rc=0, out_count=0):
    if not isdir(pjoin(check_dir, table)):
        os.mkdir(pjoin(check_dir, table))

    for check_id in range(1000):
       fqfn = pjoin(check_dir, table, 'rule_%d.bash' % check_id)
       if not isfile(fqfn):
           break

    with open(fqfn, 'w') as f:
        f.write(u'#!/usr/bin/env bash \n')
        if out_count is not None:
            f.write(u'echo "violation_cnt: %s" \n' % out_count)
        f.write(u'exit %s \n' % rc)
    st = os.stat(fqfn)
    os.chmod(fqfn, st.st_mode | stat.S_IEXEC)


def report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt):
    """
    args:
        - report - the list of namedtuples that form a results report
        - expected_check_cnt - the number of checks that must have run
        - expected_check_rc - every check must have this rc
        - expected_violation_cnt - the number of violations that each check must have received
    From the above is also derrived:
        - expected_tot_violation_cnt - which is the expected_check_cnt * expected_violation_cnt
    """
    expected_tot_violation_cnt = int(expected_check_cnt) * int(expected_violation_cnt)
    assert len(report)   == expected_check_cnt
    actual_violations_cnt = 0
    for rec in report:
        assert rec.check_rc      == expected_check_rc
        assert rec.violation_cnt == expected_violation_cnt
        actual_violations_cnt    += int(rec.violation_cnt)


def report_rec_parser(rec):
    if not rec:
        raise EmptyRecError

    fields = rec.split('|')
    if len(fields) != 4:
        raise ValueError("Not a report rec: %s" % rec)
    else:
        table = fields[0]
        check = fields[1]
        rc    = fields[2]
        cnt   = fields[3]
        return Record(table, check, rc, cnt)


class EmptyRecError(Exception):
    def __init__(self, value=None):
        self.value = value
    def __str__(self):
        return repr(self.value)






