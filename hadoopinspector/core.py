#!/usr/bin/env python34

import os, sys, time, datetime
import json
from pprint import pprint as pp
import validictory
from os.path import isdir, isfile, exists
from os.path import join as pjoin
import sqlite3



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
        #sys.stdout.write("\nTest0-a")
        #sys.__stdout__.write("\nTest0-b")
        # lets not create the registries, make it more obvious that load & create funcs must be run.
        self.full_registry  = {}
        self.db_registry    = {} # filtered to just include checks needed for a run

    def load_registry(self, fqfn):
        """
        :param fqfn     - str
        :param instance - str
        :param db       - str
        :param table    - str, optional
        :param check    - str, optional
        """
        if not isfile(fqfn):
            abort("Invalid registry file: %s" % fqfn)
        with open(fqfn) as f:
            self.full_registry = json.load(f)

    def generate_db_registry(self, instance, db, table=None, check=None):
        assert instance is not None
        assert db is not None

        if not self.full_registry:
            raise EOFError("Registry is empty")

        # just in case provided instance & db aren't in registry:
        if instance not in self.full_registry or db not in self.full_registry[instance]:
            #pp("\n\nself.full_registry: ")
            pp(self.full_registry)
            raise EOFError("No registry found for instance (%s) & db (%s)" % (instance, db) )

        for full_table in self.full_registry[instance][db]:
            if table is None or (table == full_table):
                for full_check in self.full_registry[instance][db][full_table]:
                    if check is None or (check == full_check):
                        ck = self.full_registry[instance][db][full_table][full_check]
                        self.add_check(instance, db, full_table, full_check, registry=self.db_registry, **ck)

        if len(self.db_registry.keys()) == 0:
            raise EOFError("No registry found for instance & db")


    def get_instance(self, instance, registry=None):
        if registry is None:

            registry = self.full_registry
        return registry[instance]

    def add_instance(self, instance, registry=None):
        if registry is None:
            registry = self.full_registry
        registry[instance] = {}

    def add_db(self, instance, db, registry=None):
        if registry is None:
            registry = self.full_registry
        registry[instance][db] = {}

    def list_tables(self, instance, db, registry=None):
        if registry is None:
            registry = self.full_registry
        return registry[instance][db].keys()

    def add_table(self, instance, db, table, registry=None):
        if registry is None:
            registry = self.full_registry
        registry[instance][db][table] = {}

    def list_checks(self, instance, db, table, registry=None):
        if registry is None:
            registry = self.full_registry
        return registry[instance][db][table].keys()

    def add_check(self, instance, db, table, check, check_name, check_status, check_type,
                  check_mode, check_scope, check_tags, registry=None):
        """ Adds a check structure to registry provided as arg
        """
        if registry is None:
            registry = self.full_registry
        assert isinstance(registry, dict)

        #--- add hierarchy parents:
        if instance not in registry:
            registry[instance] = {}
        if db not in registry[instance]:
            registry[instance][db] = {}
        if table not in registry[instance][db]:
            registry[instance][db][table] = {}

        #--- finally, add check:
        registry[instance][db][table][check] = {
               'check_name':    check_name,
               'check_status':  check_status,
               'check_type':    check_type,
               'check_mode':    check_mode,
               'check_scope':   check_scope,
               'check_tags':    []  }

    def write(self, filename=None, registry=None):
        if registry is None:
            registry = self.full_registry
        if not filename:
            filename = 'registry.json'
        with open(filename, 'w') as outfile:
            json.dump(registry, outfile)
        assert isfile(filename)
        return filename

    def validate_file(self, filename):

        if not isfile(filename):
            return ValueError, 'Invalid file: %s' % filename

        try:
            with open(filename) as infile:
                reg = json.load(infile)
        except ValueError:
            abort("Invalid json file")
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

        pp(registry)
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



class CheckRepo(object):

    def __init__(self, check_dir):
        self.check_dir = check_dir
        # check repo is constructed here
        # check_repo contains a dictionary of table names
        # each table name contains a dictionary of checks
        # each cehck contains a dictionary of info about that check and how to # run it
        # todo: expand on this description
        self.repo      = {}
        for check_fn in os.listdir(self.check_dir):
            self.repo[check_fn] = {}
            self.repo[check_fn]['fqfn'] = pjoin(self.check_dir, check_fn)



class CheckResults(object):

    def __init__(self):
        self.start_dt = datetime.datetime.utcnow()
        self.results = {}

    def add(self, instance, database, table, check, violations, rc,
            check_status=None,
            check_type='rule',
            check_policy_type='quality',
            check_mode='full',
            check_unit='rows',
            check_scope=-1,
            check_severity_score=-1,
            run_start_timestamp=None,
            run_stop_timestamp=None):
        assert isnumeric(rc)
        assert isnumeric(violations)
        assert check_type   in ('rule', 'profile')
        assert check_policy_type   in ('quality', 'consistency', 'data-management'), "invalid policy_type: %s" % check_policy_type
        assert check_unit   in ('rows', 'tables')
        assert check_mode   in ('full', 'incremental')
        assert check_status in (None, 'active', 'inactive')
        assert isnumeric(check_scope)
        assert isnumeric(check_severity_score)

        if instance not in self.results:
            self.results[instance] = {}
        if database not in self.results[instance]:
            self.results[instance][database] = {}
        if table not in self.results[instance][database]:
            self.results[instance][database][table] = {}
        if check not in self.results[instance][database][table]:
            self.results[instance][database][table][check] = {}

        self.results[instance][database][table][check]['violation_cnt'] = violations
        self.results[instance][database][table][check]['rc']            = rc
        self.results[instance][database][table][check]['check_status']  = check_status
        self.results[instance][database][table][check]['check_unit']    = check_unit
        self.results[instance][database][table][check]['check_type']    = check_type
        self.results[instance][database][table][check]['check_policy_type']    = check_type
        self.results[instance][database][table][check]['check_mode']           = check_mode
        self.results[instance][database][table][check]['check_scope']          = check_scope
        self.results[instance][database][table][check]['check_severity_score'] = check_severity_score
        self.results[instance][database][table][check]['run_start_timestamp']  = run_start_timestamp
        self.results[instance][database][table][check]['run_stop_timestamp']   = run_stop_timestamp

    def get_max_rc(self):
        max_rc = 0
        for instance in self.results:
            for database in self.results[instance]:
                for table in self.results[instance][database]:
                    for check in self.results[instance][database][table]:
                        if self.results[instance][database][table][check]['rc'] > max_rc:
                            max_rc = self.results[instance][database][table][check]['rc']
        return max_rc

    def get_formatted_results(self):
        formatted_results = []
        for instance in self.results:
            for database in self.results[instance]:
                for table in self.results[instance][database]:
                        for check in self.results[instance][database][table]:
                                rec = '%s|%s|%s|%s|%s|%s' % (instance, database, table, check,
                                           self.results[instance][database][table][check]['rc'],
                                           self.results[instance][database][table][check]['violation_cnt'])
                                formatted_results.append(rec)
        return formatted_results

    def write_to_sqlite(self, fqfn):
        if not isfile(fqfn):
            create_sqlite_db(fqfn)

        stop_dt = datetime.datetime.utcnow()

        conn = sqlite3.connect(fqfn)
        cur  = conn.cursor()
        recs = []

        sql  = """INSERT INTO check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  """
        for instance in self.results:
            for database in self.results[instance]:
                for table in self.results[instance][database]:
                    for check in self.results[instance][database][table]:
                         recs.append( (instance, database, table, check,
                                       self.results[instance][database][table][check]['check_type'],
                                       self.results[instance][database][table][check]['check_policy_type'],
                                       self.results[instance][database][table][check]['check_mode'],
                                       self.results[instance][database][table][check]['check_unit'],
                                       self.results[instance][database][table][check]['check_status'],
                                       (self.results[instance][database][table][check]['run_start_timestamp'] or self.start_dt),
                                       (self.results[instance][database][table][check]['run_stop_timestamp']  or stop_dt),
                                       self.results[instance][database][table][check]['rc'],
                                       self.results[instance][database][table][check]['check_scope'],
                                       self.results[instance][database][table][check]['check_severity_score'],
                                       self.results[instance][database][table][check]['violation_cnt'],

) )
        if recs:
            cur.executemany(sql, recs)
            conn.commit()
        conn.close()


def create_sqlite_db(fqfn):
    conn = sqlite3.connect(fqfn)
    cmd = """ \
            CREATE TABLE check_results  ( \
            instance_name       TEXT,  \
            database_name       TEXT,  \
            table_name          TEXT,  \
            check_name          TEXT,  \
            check_type          TEXT,  \
            check_policy_type   TEXT,  \
            check_mode          TEXT,  \
            check_unit          TEXT,  \
            check_status        TEXT,  \
            run_start_timestamp TIMESTAMP, \
            run_stop_timestamp  TIMESTAMP, \
            check_rc            INT,   \
            check_scope         INT,   \
            check_severity_score INT,   \
            check_violation_cnt INT ) """
    c    = conn.cursor()
    c.execute(cmd)
    conn.commit()
    conn.close()



def abort(msg=""):
    print(msg)
    sys.exit(1)

def isnumeric(val):
    try:
        int(val)
    except TypeError:
        return False
    except ValueError:
        return False
    else:
        return True





