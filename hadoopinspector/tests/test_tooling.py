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

Record = collections.namedtuple('Record', 'table check check_rc violation_cnt')



def add_to_registry(registry_fn, inst, db, table, check_dir, check_fn,
                    check_type='rule', **checkvars):

   check_alias  = os.path.splitext(basename(check_fn))[0]
   check_status = 'active'
   check_mode   = 'full'
   check_scope  = 'row'
   reg = core.Registry()
   if registry_fn and isfile(registry_fn):
       reg.load_registry(registry_fn)
   reg.add_check(inst, db, table, check_alias, basename(check_fn),
                 check_status, check_type, check_mode, check_scope,
                 **checkvars)
   registry_fn = reg.write(registry_fn)
   assert isfile(registry_fn)
   return registry_fn


def add_check(check_dir, table, rc, out_count=0, formatter_fqfn=None):
    for check_id in range(1000):
        fqfn = pjoin(check_dir, 'check_%s_%d.bash' % (table, check_id))
        if not isfile(fqfn):
            break
    if formatter_fqfn is None:
        formatter_fqfn = pjoin(script_path, 'hapinsp_formatter.py')

    with open(fqfn, 'w') as f:
        f.write(u'#!/usr/bin/env bash \n')
        f.write(""" echo `%s --rc 0 --violation-cnt %s` \n""" % (formatter_fqfn, out_count))
        f.write(u'exit %s \n' % rc)
    st = os.stat(fqfn)
    os.chmod(fqfn, st.st_mode | stat.S_IEXEC)
    return fqfn


def add_env_check(check_dir, table, key, value, formatter_fqfn=None):
    """ Add a check that returns violations based on match of key & value to
        env variables.
        Return: fqfn of check
    """
    for check_id in range(1000):
       fqfn = pjoin(check_dir, 'check_%s_%d.bash' % (table, check_id))
       if not isfile(fqfn):
           break
    if formatter_fqfn is None:
        formatter_fqfn = pjoin(script_path, 'hapinsp_formatter.py')
    with open(fqfn, 'w') as f:
        f.write("""#!/usr/bin/env bash \n """ )
        f.write('\n')
        f.write(""" if [ "$%s" = "%s"  ]; then \n""" % (key, value))
        f.write("""     outcount=0 \n""")
        f.write(""" else \n""")
        f.write("""     outcount=1 \n""")
        f.write(""" fi \n""")
        f.write(""" echo `%s --rc 0 --violation-cnt $outcount ` \n""" % formatter_fqfn)
        f.write(""" exit 0 \n """)
    st = os.stat(fqfn)
    os.chmod(fqfn, st.st_mode | stat.S_IEXEC)
    return fqfn


def add_setup_check(check_dir, key=None, value=None, required_key=None, required_value=None,
                    fqfn=None, formatter_fqfn=None):

    if not fqfn:
        for check_id in range(1000):
            fqfn = pjoin(check_dir, 'setup_check_%d.bash' % check_id)
            if not isfile(fqfn):
                break
    if formatter_fqfn is None:
        formatter_fqfn = pjoin(script_path, 'hapinsp_formatter.py')
    with open(fqfn, 'w') as f:
        f.write("""#!/usr/bin/env bash \n""" )
        f.write('\n')
        if key:
            f.write("""echo `%s --rc 0 --kv "%s:%s"` \n""" % (formatter_fqfn, key, value))
            f.write("""exit 0 \n""")
        elif required_key:
            #f.write("""echo $hapinsp_tablecustom_foo_prior \n""")
            #f.write("""env""")
            f.write("""if [ "$%s" = "%s"  ]; then \n""" % (required_key, required_value))
            f.write("""    echo `%s --rc 0  ` \n""" % formatter_fqfn)
            f.write("""    exit 0 \n""")
            f.write("""else \n""")
            f.write("""    echo `%s --rc 1  ` \n""" % formatter_fqfn)
            f.write("""    exit 1 \n""")
            f.write("""fi \n""")
        else:
            print("Error: invalid input to add_setup_check")
            sys.exit(1)
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
    for rec in report:
        if rec.check.startswith('setup'):
            assert rec.violation_cnt == ''
        else:
            assert rec.check_rc      == str(expected_check_rc)
            assert rec.violation_cnt == str(expected_violation_cnt)


def report_rec_parser(rec):
    if not rec:
        raise EmptyRecError

    fields = rec.split('|')
    if len(fields) != 5:
        raise ValueError("Not a report rec: %s" % rec)
    else:
        table = fields[0]
        check = fields[1]
        rc    = fields[3]
        cnt   = fields[4]
        return Record(table, check, rc, cnt)




def get_formatter_fqfn():

    fqfn = (find_file('.', 'hapinsp_formatter.py')
             or find_file('../', 'hapinsp_formatter.py')
             or find_file('../../', 'hapinsp_formatter.py'))
    if fqfn is None:
        print('Missing hapinsp_formatter.py - Aborting.')
        sys.exit(0)
    else:
        return fqfn

def find_file(path, file_to_find):
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename == file_to_find:
                return os.path.join(dirname, filename)


def print_check_results(results_fqfn):

    conn = sqlite3.connect(results_fqfn)
    c = conn.cursor()
    print("--- sqlite check_results database contents: --- ")
    #sql = "select * from check_results"
    #c.execute(sql)
    #pp(c.fetchall())
    sql = "select run_start_timestamp, check_name, setup_vars from check_results order by 1"
    c.execute(sql)
    pp(c.fetchall())
    conn.close()



def print_file(filename):
    with open(filename, 'r') as f:
         print(f.read())





class EmptyRecError(Exception):
    def __init__(self, value=None):
        self.value = value
    def __str__(self):
        return repr(self.value)

