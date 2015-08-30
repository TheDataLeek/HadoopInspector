#!/usr/bin/env python34
from __future__ import division
import sys, os, shutil, stat
import tempfile, json
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












