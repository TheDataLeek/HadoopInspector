#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""

import os, sys, time, subprocess
import json, logging
from os.path import isdir, isfile, exists, dirname, basename
from os.path import join as pjoin
import errno
from pprint import pprint as pp

import validictory
import demjson


class Registry(object):
    """ Sample Config File, with just a single check for a single table
    for a single db for a single instance.  Normally, a config file would
    contain multiple tables.
    {
        "asset": {              # table-name
            "rule_pk1": {       # check-name
                "check_type":   "rule",
                "check_name":   "rule_uniqueness",
                "check_mode":   "full",
                "check_scope":  "row",
                "check_status": "active"
                "hapinsp_checkcustom_cols": date_id
            }
        }
    }
    """

    def __init__(self):
        self.registry       = {}
        self.logger = logging.getLogger('RunnerLogger')

    def _abort(self, msg):
        if self.logger:
            self.logger.critical(msg)
        else:
            print(msg)
        sys.exit(1)

    def load_registry(self, fqfn):
        """load registry json file into self.registry.

        Does no validation other than requiring the file to be valid json.

        :param fqfn     - str
        """
        if not isfile(fqfn):
            self._abort("Invalid registry file: %s" % fqfn)

        with open(fqfn) as infile:
            json_str = infile.read()
            try:
                self.registry, reg_errors, reg_stats = demjson.decode(json_str, return_errors=True)
            except demjson.JSONDecodeError as e:
                self.logger.critical("registry json load error: %s", e)
                for err in reg_errors:
                    self.logger.critical(err)
                self._abort("Invalid registry file - could not load/decode")
            else:
                if reg_errors:
                    self.logger.critical("registry json load error")
                    for err in reg_errors:
                        self.logger.critical(err)
                    self._abort("Invalid registry file - json errors discovered during load")

    def filter_registry(self, filter_table=None, filter_check=None):
        """ Filter the registry to only the table and check provided.

        :param table    - str, optional
        :param check    - str, optional
        """
        if not self.registry:
            self.logger.critical('invalid registry file - it is empty')
            raise EOFError("Registry is empty")

        new_reg = {}
        for loaded_table in self.registry:
            if filter_table is None and filter_check is None:
                new_reg[loaded_table] = self.registry[loaded_table]
            elif filter_table == loaded_table and filter_check is None:
                new_reg[loaded_table] = self.registry[loaded_table]
            elif filter_table == loaded_table and filter_check:
                for loaded_check in self.registry[loaded_table]:
                    if loaded_check == filter_check:
                        if loaded_table not in new_reg:
                            new_reg[loaded_table] = {}
                        new_reg[loaded_table][loaded_check] = self.registry[loaded_table][filter_check]
            elif filter_table is None and filter_check:
                for loaded_check in self.registry[loaded_table]:
                    if loaded_check == filter_check:
                        if loaded_table not in new_reg:
                            new_reg[loaded_table] = {}
                        new_reg[loaded_table][loaded_check] = self.registry[loaded_table][filter_check]
        self.registry = new_reg

    def add_table(self, table):
        self.registry[table] = {}

    def add_check(self, table, check, check_name, check_status, check_type,
                  check_mode, check_scope, **checkvars):
        """ Add a check structure to registry.  If no registry is provided,
            then it'll add this to the registry.
        """
        if table not in self.registry:
            self.add_table(table)

        #--- finally, add check:
        self.registry[table][check] = {
               'check_name':    check_name,
               'check_status':  check_status,
               'check_type':    check_type,
               'check_mode':    check_mode,
               'check_scope':   check_scope }
        for key in checkvars:
            if not key.startswith('hapinsp_checkcustom_'):
                self.logger.critical("Invalid registry check (%s) - invalid checkvar (%s)", check, key)
                self._abort("Invalid registry checkvar: %s" % key)
            self.registry[table][check][key] = checkvars[key]

    def add_setup_check(self, table, check, check_name, check_status, check_type,
                  check_mode, **checkvars):
        """ Add a check structure to registry.  If no registry is provided,
            then it'll add this to the registry.
        """
        if table not in self.registry:
            self.add_table(table)

        #--- finally, add check:
        self.registry[table][check] = {
               'check_name':    check_name,
               'check_status':  check_status,
               'check_type':    check_type,
               'check_mode':    check_mode }
        for key in checkvars:
            if not key.startswith('hapinsp_checkcustom_'):
                self.logger.critical("Invalid registry check (%s) - invalid checkvar (%s)", check, key)
                self._abort("Invalid registry checkvar: %s" % key)
            self.registry[table][check][key] = checkvars[key]

    def write(self, filename=None, registry=None):
        if registry is None:
            registry = self.registry
        if not filename:
            filename = 'registry.json'
        with open(filename, 'w') as outfile:
            json.dump(registry, outfile)
        assert isfile(filename)
        return filename

    def validate_file(self, filename):

        if not isfile(filename):
            print('validate_file - b')
            return ValueError, 'Invalid file: %s' % filename

        try:
            with open(filename) as infile:
                json_str = infile.read()
                try:
                    self.registry, reg_errors, reg_stats = demjson.decode(json_str, return_errors=True)
                except demjson.JSONDecodeError as e:
                    self.logger.critical("registry json validation error: %s", e)
                    for err in reg_errors:
                        self.logger.critical(err)
                    self._abort("Invalid registry file - could not decode")
                else:
                    if reg_errors:
                        self.logger.critical("registry json validation error")
                        for err in reg_errors:
                            self.logger.critical(err)
                        self._abort("Invalid registry file - json errors discovered")
        except IOError:
            self._abort("Invalid registry file - could not open")

        try:
            self.validate()
        except:
            self.logger.critical("registry file validation failed")
            raise


    def default(self):
        for table in self.registry:
            for check in self.registry[table]:
                checkobj = self.registry[table][check]

                if checkobj.get('check_type', None) is None:
                    checkobj['check_type'] = 'rule'

                if checkobj['check_type'] == 'setup':
                    if 'check_mode' not in checkobj:
                        checkobj['check_mode'] = None
                    if 'check_scope' not in checkobj:
                        checkobj['check_scope'] = None
                elif checkobj['check_type'] == 'rule':
                    if 'check_mode' not in checkobj:
                        checkobj['check_mode'] = 'full'
                    if 'check_scope' not in checkobj:
                        checkobj['check_scope'] = 'row'

                if checkobj.get('check_status', None) is None:
                    checkobj['check_status'] = 'active'


    def validate(self):

        regular_check_schema = {
             "type": "object",
             "properties": {
                    "check_name":   {"type": "string"},
                    "check_status": {"type": "string",
                                    "enum": ["active", "inactive"] },
                    "check_type":   {"type": "string",
                                    "enum": ["rule", "profile"] },
                    "check_mode":   {"type": "string",
                                    "enum": ["full", "incremental"] },
                    "check_scope":  {"type": "string",
                                    "enum": ["row", "table", "database"] }
                           }
        }
        setupteardown_check_schema = {
             "type": "object",
             "properties": {
                    "check_name":   {"type": "string"},
                    "check_status": {"type": "string",
                                    "enum": ["active", "inactive"] },
                    "check_type":   {"type": "string",
                                    "enum": ["setup", "teardown"] },
                    "check_mode":   {"type": "any"},
                    "check_scope":  {"type": "null"}
                           }
        }

        def validate_check(check_reg, check_type):
            assert check_type in ['rule', 'profile', 'setup', 'teardown']
            try:
                if check_type in ['setup', 'teardown']:
                    validictory.validate(check_reg, setupteardown_check_schema)
                else:
                    validictory.validate(check_reg, regular_check_schema)
            except validictory.validator.RequiredFieldValidationError as e:
                self._abort("Registry error on field: %s" % e)
            except validictory.FieldValidationError as e:
                self._abort("Registry error on field: %s with value: %s with check_type: %s" \
                     % (e.fieldname, check_reg[e.fieldname], check_type))
            except:
                self._abort("Error encountered while processing Registry")

        if not isinstance(self.registry, dict):
            self._abort(msg="Invalid registry")
        for table in self.registry:
            if not isinstance(self.registry[table], dict):
                self._abort(msg="Invalid registry table: %s" % table)
            for check in self.registry[table]:
                check_type = self.registry[table][check].get('check_type', None)
                if check_type is None:
                    self._abort(msg="Missing check_type for: %s" % table )
                else:
                    try:
                        validate_check(self.registry[table][check], check_type)
                    except:
                        self.logger.debug('table: %s', table)
                        self.logger.debug('check: %s', check)
                        self.logger.debug(self.registry[table][check])
                        self.logger.critical('table: %s check: %s failure!', table, check)
                        raise
