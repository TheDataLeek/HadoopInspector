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


def valid_iso8601(iso8601, isotype):
    """ Verifies that input iso8601-format is valid
    Args:
        iso8601 (str): must have one of the following formats:
            yyyy-mm-ddThh:mm:ss
            yyyy-mm-ddThh:mm:ss.mmmmmm
            yyyymmddThhmmss
            yyyymmddThhmmssmmmmmm
        isotype (str): defaults to 'any', but can have any of the following formats:
            'basic'
            'ext'
            'any'
    Returns:
        dt (datetime.datetime): or None if iso8601 is None
    """

    assert isotype in ('basic', 'ext', 'any')
    if iso8601 is None:
        return False
    if isotype == 'any':
        isotype = 'ext' if len(iso8601) in (19, 26) else 'basic'

    try:
        if isotype == 'ext':
            if len(iso8601) == 19:
                datetime.datetime.strptime(iso8601, "%Y-%m-%dT%H:%M:%S")
            elif len(iso8601) == 26:
                datetime.datetime.strptime(iso8601, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                return False
        else:
            if len(iso8601) == 15:
                datetime.datetime.strptime(iso8601, "%Y%m%dT%H%M%S")
            elif len(iso8601) == 21:
                datetime.datetime.strptime(iso8601, "%Y%m%dT%H%M%S%f")
            else:
                return False
    except (ValueError, TypeError):
        return False
    else:
        return True

