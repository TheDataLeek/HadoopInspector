#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""

import os, sys, time, datetime, subprocess
import json, copy, logging
from os.path import isdir, isfile, exists, dirname, basename
from os.path import join as pjoin
import errno
from pprint import pprint as pp

import sqlite3


class CheckRepo(object):
    """ Maintains information about actual checks.

    """
    def __init__(self, check_dir):
        self.check_dir = check_dir
        self.repo = {}
        self.logger = logging.getLogger('RunnerLogger')
        for check_fn in os.listdir(self.check_dir):
            self.repo[check_fn] = {}
            self.repo[check_fn]['fqfn'] = pjoin(self.check_dir, check_fn)




def isnumeric(val):
    try:
        int(val)
    except TypeError:
        return False
    except ValueError:
        return False
    else:
        return True


