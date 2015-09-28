#!/usr/bin/env python2
from __future__ import division
import sys, os
from os.path import join as pjoin
from os.path import dirname

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import hadoopinspector_demogen as mod


class TestGetScope(object):

    def test_nonpartitioned_table(self):
        assert mod.get_scope(partition_key=None,
                             avg_row_cnt=5,
                             violation_unit='tables',
                             violation_cnt=5) == 100
        assert mod.get_scope(partition_key=None,
                             avg_row_cnt=5,
                             violation_unit='tables',
                             violation_cnt=0) == 0
        assert mod.get_scope(partition_key=None,
                             avg_row_cnt=5,
                             violation_unit='tables',
                             violation_cnt=1) == 100


    def test_nonpartitioned_rows(self):
        assert mod.get_scope(partition_key=None,
                             avg_row_cnt=5,
                             violation_unit='rows',
                             violation_cnt=5) == 100
        assert mod.get_scope(partition_key=None,
                             avg_row_cnt=5,
                             violation_unit='rows',
                             violation_cnt=0) == 0
        assert mod.get_scope(partition_key=None,
                             avg_row_cnt=5,
                             violation_unit='rows',
                             violation_cnt=1) == 20


    def test_partitioned_rows(self):
        assert mod.get_scope(partition_key='day',
                             avg_row_cnt=5,
                             violation_unit='rows',
                             violation_cnt=5) == 1
        assert mod.get_scope(partition_key='day',
                             avg_row_cnt=5,
                             violation_unit='rows',
                             violation_cnt=0) == 0
        assert mod.get_scope(partition_key='day',
                             avg_row_cnt=5,
                             violation_unit='rows',
                             violation_cnt=1) == 0.2
