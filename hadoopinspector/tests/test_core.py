#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""
from __future__ import division
import sys, os, shutil, stat
import logging
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

sys.path.insert(0, dirname(dirname(dirname(os.path.abspath(__file__)))))
sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))

import hadoopinspector.core as mod

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
        self.repo = mod.CheckRepo(self.check_dir)

        assert len(self.repo.repo) == 3
        for check in self.repo.repo:
            assert isfile(self.repo.repo[check]['fqfn'])



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


