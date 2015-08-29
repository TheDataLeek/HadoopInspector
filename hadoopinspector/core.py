#!/usr/bin/env python34

import os, sys, time
import json
from pprint import pprint as pp
import validictory
from os.path import isdir, isfile, exists



class Registry(object):
    """
    Sample Config File, with just a single check for a single table 
    for a single db for a single instance:
    {
        "prod1": {                      # instance-name
            "AssetEvent": {             # db-name
                "asset": {              # table-name
                    "rule_pk1": {       # check-name
                        "check_type":   "rule",
                        "check_name":   "rule_uniqueness",
                        "check_tags":   [],
                        "check_mode":   "full",
                        "check_scope":  "row",
                        "check_status": "active" 
                    }
                }
            }
        }
    }
    """

    def __init__(self):
        self.registry = {}

    def load_registry(self, fqfn, instance=None, db=None, table=None, check=None):
        """ 
        :param fqfn - str
        :param instance - str, optional
        :param db - str, optional
        :param table - str, optional
        :param check - str, optional
           
        if not isfile(fqfn):
            abort("Invalid registry file: %s" % fqfn)
        with open(fqfn) as f:
            self.registry = json.load(f)
  
        

    def get_instance(self, instance):
        return self.registry[instance]

    def add_instance(self, instance):
        self.registry[instance] = {}

    def get_db(self, instance, db):
        return self.registry[instance][db]

    def add_db(self, instance, db):
        self.registry[instance] = {}
        self.registry[instance][db] = {}

    def list_tables(self, instance, db):
        return self.registry[instance][db].keys()

    def get_table(self, instance, db, table):
        return self.registry[instance][db][table]

    def add_table(self, instance, db, table):
        self.registry[instance][db] = {}
        self.registry[instance][db][table] = {}

    def list_checks(self, instance, db, table):
        return self.registry[instance][db][table].keys()

    def get_check(self, instance, db, table, check):
        return self.registry[instance][db][table][check]

    def add_check(self, instance, db, table, check, check_name, check_status, check_type,
                  check_mode, check_scope, check_tags):

        self.registry[instance][db][table] = {}
        self.registry[instance][db][table][check] = {
               'check_name':    check_name,
               'check_status':  check_status,
               'check_type':    check_type,
               'check_mode':    check_mode,
               'check_scope':   check_scope,
               'check_tags':    []  }

    def write(self, filename=None):
        if not filename:
            filename = 'registry.json'
        with open(filename, 'w') as outfile:
            json.dump(self.registry, outfile)

    def validate_file(self, filename):

        if not isfile(filename):
            return ValueError, 'Invalid file: %s' % filename

        with open(filename) as infile:
            reg = json.load(infile)
        self.validate(reg)

    def validate(self, registry):

        check_schema = {
             "type": "object",
             "properties": {
                    "check_name":   {"type": "string"},
                    "check_status": {"type": "string",
                                    "enum": ["active", "inactive"] },
                    "check_type":   {"type": "string",
                                    "enum": ["rule", "profiler"] },
                    "check_mode":   {"type": "string",
                                    "enum": ["full", "part"] },
                    "check_scope":  {"type": "string",
                                    "enum": ["row", "table", "database"] },
                    "check_tags":   {"type": "array"}
                           }
        }

        def validate_check(check_reg):
            try:
                validictory.validate(check_reg, check_schema)
            except validictory.validator.RequiredFieldValidationError as e:
                abort("Registry error on field: %s" % e)
            except validictory.FieldValidationError as e:
                abort("Registry error on field: %s" % e.fieldname)
            except:
                abort("Error encountered while processing Registry")

        if not isinstance(registry, dict):
            abort(msg="Invalid registry")
        for instance in registry:
            if not isinstance(registry[instance], dict):
                abort(msg="Invalid registry instance: %s" % instance)
            for db in registry[instance]:
                if not isinstance(registry[instance][db], dict):
                    abort(msg="Invalid registry db: %s.%s" % (instance, db))
                for table in registry[instance][db]:
                    if not isinstance(registry[instance][db][table], dict):
                        abort(msg="Invalid registry table: %s.%s.%s" % (instance, db, table))
                    for check in registry[instance][db][table]:
                        validate_check(registry[instance][db][table][check])


def abort(msg=""):
    print(msg)
    sys.exit(1)



