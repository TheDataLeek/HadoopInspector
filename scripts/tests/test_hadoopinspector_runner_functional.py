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

Record = collections.namedtuple('Record', 'table check check_rc violation_cnt')


class TestWithMockedCheckFiles(object):

    def setup_method(self, method):
        self.log_dir   = tempfile.mkdtemp(prefix='hadinsp_l_')
        self.check_dir = tempfile.mkdtemp(prefix='hadinsp_c_')

    def teardown_method(self, method):
        shutil.rmtree(self.check_dir)
        shutil.rmtree(self.log_dir)

    def run_cmd(self, table=None):
        cmd = [pgm,
               '--instance', 'inst1',
               '--database', 'db1',
               '--check-dir', self.check_dir,
               '--log-dir', self.log_dir,
               '--report' ]
        if table:
            cmd.extend(['--table', table])

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True)
        results =  p.communicate()[0].decode()
        run_rc = p.returncode
        report = []
        for line in results.split('\n'):
            try:
                rec = report_rec_parser(line)
                report.append(rec)
            except EmptyRecError:
                continue
            except ValueError:
                print('could not parse output rec')
                raise
        return report, run_rc


    def test_one_successful_check_with_no_violations(self):
        table = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_one_failed_check_with_no_violations(self):
        table = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '1'
        expected_run_rc        = '1'
        expected_violation_cnt = '0'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_one_successful_checks_with_violations(self):
        table = 'customer'
        expected_check_cnt      = 1
        expected_check_rc       = '0'
        expected_run_rc         = '0'
        expected_violation_cnt  = '9'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_2_successful_checks_with_no_violations(self):
        table                  = 'customer'
        expected_check_cnt     = 2
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_2_tables(self):
        #--- common to all checks against both tables: ---
        expected_check_cnt     = 4 # 2 tables with 2 checks each
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'

        table                  = 'customer'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        table                  = 'asset'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc

    def test_2_tables_with_1_required(self):
        #--- common to all checks against both tables: ---
        expected_check_cnt     = 2 # 2 tables with 2 checks for just one
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'

        table                  = 'customer'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        table                  = 'asset'
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)
        add_check(self.check_dir, table, rc=expected_check_rc, out_count=expected_violation_cnt)

        report, run_rc = self.run_cmd(table='asset')

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc




def add_check(check_dir, table, rc, out_count):
    if not isdir(pjoin(check_dir, table)):
        os.mkdir(pjoin(check_dir, table))

    for check_id in range(1000):
       fqfn = pjoin(check_dir, table, 'check_%d.bash' % check_id)
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






