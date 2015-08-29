#!/usr/bin/env python34
from __future__ import division
import sys, os, shutil, stat
import tempfile, json
import subprocess
import copy
import collections
from pprint import pprint as pp
from os.path import exists, isdir, isfile
from os.path import join as pjoin
from os.path import dirname
import pytest

script_path = os.path.dirname(os.path.dirname(os.path.realpath((__file__))))
pgm         = os.path.join(script_path, 'hadoopinspector_runner.py')

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))

import core as mod

Record = collections.namedtuple('Record', 'table check check_rc violation_cnt')



class TestRegistry(object):

    def setup_method(self, method):
        self.temp_dir = tempfile.mkdtemp(prefix='hadinsp_')

    def teardown_method(self, method):
        shutil.rmtree(self.temp_dir)


    def test_creating_then_writing(self):
        self.registry = mod.Registry()
        self.registry.add_instance('prod1')
        self.registry.add_db('prod1', 'AssetEvent')
        self.registry.add_table('prod1', 'AssetEvent', 'asset')
        self.registry.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row',
               check_tags=[])
        self.registry.write(pjoin(self.temp_dir, 'registry.json'))
        self.registry.validate_file(pjoin(self.temp_dir, 'registry.json'))
        #with open(pjoin(self.temp_dir, 'registry.json')) as f:
        #    pp(f.read())

    def test_creating_then_loading(self):
        self.registry = mod.Registry()
        self.registry.add_instance('prod1')
        self.registry.add_db('prod1', 'AssetEvent')
        self.registry.add_table('prod1', 'AssetEvent', 'asset')
        self.registry.add_check('prod1', 'AssetEvent', 'asset', 'rule_pk1',
               check_name='rule_uniqueness',
               check_status='active',
               check_type='rule',
               check_mode='full',
               check_scope='row',
               check_tags=[])
        self.registry.write(pjoin(self.temp_dir, 'registry.json'))
        self.registry2 = mod.Registry()
        self.registry2.load_registry(pjoin(self.temp_dir, 'registry.json'))
        assert self.registry.registry == self.registry2.registry

    def test_validation(self):
        self.registry = mod.Registry()
        self.registry.registry = 'foo'
        self.registry.write(pjoin(self.temp_dir, 'registry.json'))
        with pytest.raises(SystemExit):
            self.registry.validate_file(pjoin(self.temp_dir, 'registry.json'))




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











