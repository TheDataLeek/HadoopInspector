#!/usr/bin/env python34
from __future__ import division
import sys, os, shutil, stat
import tempfile
import subprocess
import collections
from pprint import pprint as pp

from os.path import exists, isdir, isfile, basename, dirname
from os.path import join as pjoin
from os.path import dirname

script_path = os.path.dirname(os.path.dirname(os.path.realpath((__file__))))
pgm         = os.path.join(script_path, 'hadoopinspector_runner.py')

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(os.path.abspath(__file__)))))
import hadoopinspector.core as core

Record = collections.namedtuple('Record', 'instance db table check check_rc violation_cnt')


class TestWithMockedCheckFiles(object):

    def setup_method(self, method):
        self.log_dir   = tempfile.mkdtemp(prefix='hadinsp_l_')
        self.check_dir = tempfile.mkdtemp(prefix='hadinsp_c_')
        self.registry_fqfn = None
        self.inst      = 'inst1'
        self.db        = 'db1'

    def teardown_method(self, method):
        shutil.rmtree(self.check_dir)
        shutil.rmtree(self.log_dir)

    def run_cmd(self, table=None):
        assert isfile(self.registry_fqfn)
        assert isdir(self.check_dir)
        assert isdir(self.log_dir)
        cmd = [pgm,
               '--instance', self.inst,
               '--database', self.db,
               '--registry-filename', self.registry_fqfn,
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
            print(line)
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
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        check_fqfn = add_check(self.check_dir, rc=expected_check_rc,
                               out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                             table, self.check_dir, check_fqfn)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_one_failed_check_with_no_violations(self):
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '1'
        expected_run_rc        = '1'
        expected_violation_cnt = '0'
        check_fqfn  = add_check(self.check_dir, rc=expected_check_rc,
                                out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_one_successful_checks_with_violations(self):
        table                   = 'customer'
        expected_check_cnt      = 1
        expected_check_rc       = '0'
        expected_run_rc         = '0'
        expected_violation_cnt  = '9'
        check_fqfn  = add_check(self.check_dir, rc=expected_check_rc,
                                out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_2_successful_checks_with_no_violations(self):
        table                  = 'customer'
        expected_check_cnt     = 2
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'

        check_fqfn1  = add_check(self.check_dir, rc=expected_check_rc,
                                 out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn1)

        check_fqfn2 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn2)

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
        check_fqfn1 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn1)

        check_fqfn2 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn2)

        table                  = 'asset'
        check_fqfn3 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn3)

        check_fqfn4 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn4)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_2_tables_with_1_required(self):
        #--- common to all checks against both tables: ---
        expected_check_cnt     = 2 # 2 tables with 2 checks for just one
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'

        # no tests should get run against these two - since they aren't in
        # registry:
        table                  = 'customer'
        check_fqfn1 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        check_fqfn2 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)

        table                  = 'asset'
        check_fqfn1 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn1)
        check_fqfn2 = add_check(self.check_dir, rc=expected_check_rc, out_count=expected_violation_cnt)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                                             table, self.check_dir, check_fqfn2)

        report, run_rc = self.run_cmd(table='asset')

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc


    def test_check_env_variable_table(self):
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        check_fqfn = add_env_check(self.check_dir, key='hapinsp_table', value='customer')
        os.system("cat %s" % check_fqfn)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                             table, self.check_dir, check_fqfn)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc

    def test_check_env_variables_database(self):
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        check_fqfn = add_env_check(self.check_dir, key='hapinsp_database', value=self.db)
        os.system("cat %s" % check_fqfn)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                             table, self.check_dir, check_fqfn)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc

    def test_check_env_variables_instance(self):
        table                  = 'customer'
        expected_check_cnt     = 1
        expected_check_rc      = '0'
        expected_run_rc        = '0'
        expected_violation_cnt = '0'
        check_fqfn = add_env_check(self.check_dir, key='hapinsp_instance', value=self.inst)
        os.system("cat %s" % check_fqfn)
        self.registry_fqfn = add_to_registry(self.registry_fqfn, self.inst, self.db,
                             table, self.check_dir, check_fqfn)

        report, run_rc = self.run_cmd()

        report_checker(report, expected_check_cnt, expected_check_rc, expected_violation_cnt)
        assert str(run_rc) == expected_run_rc






def add_to_registry(registry_fn, inst, db, table, check_dir, check_fn):

   check_alias  = os.path.splitext(basename(check_fn))[0]
   check_status = 'active'
   check_type   = 'rule'
   check_mode   = 'full'
   check_scope  = 'row'
   check_tags   = []
   reg = core.Registry()
   if registry_fn and isfile(registry_fn):
       reg.load_registry(registry_fn)
   reg.add_check(inst, db, table, check_alias, basename(check_fn),
                 check_status, check_type, check_mode, check_scope, check_tags)
   registry_fn = reg.write(registry_fn)
   assert isfile(registry_fn)
   return registry_fn


def add_check(check_dir, rc, out_count):

    for check_id in range(1000):
       fqfn = pjoin(check_dir, 'check_%d.bash' % check_id)
       if not isfile(fqfn):
           break

    with open(fqfn, 'w') as f:
        f.write(u'#!/usr/bin/env bash \n')
        if out_count is not None:
            f.write(u'echo "violation_cnt: %s" \n' % out_count)
        f.write(u'exit %s \n' % rc)
    st = os.stat(fqfn)
    os.chmod(fqfn, st.st_mode | stat.S_IEXEC)
    return fqfn


def add_env_check(check_dir, key, value):

    for check_id in range(1000):
       fqfn = pjoin(check_dir, 'check_%d.bash' % check_id)
       if not isfile(fqfn):
           break

    with open(fqfn, 'w') as f:
        f.write("""#!/usr/bin/env bash \n """ )
        f.write('\n')
        f.write(""" if [ "$%s" = "%s"  ]; then \n""" % (key, value))
        f.write("""     outcount=0 \n""")
        f.write(""" else \n""")
        f.write("""     outcount=1 \n""")
        f.write(""" fi \n""")
        f.write(""" echo "violation_cnt: $outcount " \n""" )
        f.write(""" exit 0 \n """)
    st = os.stat(fqfn)
    os.chmod(fqfn, st.st_mode | stat.S_IEXEC)
    return fqfn



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
        print("report_checker.rec: ")
        pp(rec)
        assert rec.check_rc      == expected_check_rc
        assert rec.violation_cnt == expected_violation_cnt
        actual_violations_cnt    += int(rec.violation_cnt)


def report_rec_parser(rec):
    if not rec:
        raise EmptyRecError

    fields = rec.split('|')
    if len(fields) != 6:
        raise ValueError("Not a report rec: %s" % rec)
    else:
        inst  = fields[0]
        db    = fields[1]
        table = fields[2]
        check = fields[3]
        rc    = fields[4]
        cnt   = fields[5]
        return Record(inst, db, table, check, rc, cnt)


class EmptyRecError(Exception):
    def __init__(self, value=None):
        self.value = value
    def __str__(self):
        return repr(self.value)






