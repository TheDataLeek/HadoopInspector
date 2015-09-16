#!/usr/bin/env python34

import os, sys, time, datetime, subprocess
import json
from pprint import pprint as pp
import validictory
from os.path import isdir, isfile, exists
from os.path import join as pjoin
import sqlite3



class Registry(object):

    """ Sample Config File, with just a single check for a single table
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
        """
        if not isfile(fqfn):
            abort("Invalid registry file: %s" % fqfn)
        with open(fqfn) as f:
            self.full_registry = json.load(f)

    def generate_db_registry(self, instance, db, table=None, check=None):
        """ Generate the db registry from the full registry.  Optionally, only
        include specified table and/or check

        :param instance - str
        :param db       - str
        :param table    - str, optional
        :param check    - str, optional
        """

        assert instance is not None
        assert db is not None

        if not self.full_registry:
            raise EOFError("Registry is empty")

        # just in case provided instance & db aren't in loaded full_registry:
        if instance not in self.full_registry or db not in self.full_registry[instance]:
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
        """ Add a check structure to registry.  If no registry is provided,
            then it'll add this to the full_registry.
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

        regular_check_schema = {
             "type": "object",
             "properties": {
                    "check_name":   {"type": "string"},
                    "check_status": {"type": "string",
                                    "enum": ["active", "inactive"] },
                    "check_type":   {"type": "string",
                                    "enum": ["rule", "profile"] },
                    "check_mode":   {"type": "string",
                                    "enum": ["full", "part"] },
                    "check_scope":  {"type": "string",
                                    "enum": ["row", "table", "database"] },
                    "check_tags":   {"type": "array"}
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
                    "check_mode":   {"type": "null"},
                    "check_scope":  {"type": "null"},
                    "check_tags":   {"type": "array"}
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
                pp(check_reg)
                abort("Registry error on field: %s" % e)
            except validictory.FieldValidationError as e:
                pp(check_reg)
                abort("Registry error on field: %s with value: %s with check_type: %s" \
                     % (e.fieldname, check_reg[e.fieldname], check_type))
            except:
                abort("Error encountered while processing Registry")

        #pp(registry)
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
                        check_type = registry[instance][db][table][check].get('check_type', None)
                        if check_type is None:
                            abort(msg="Missing check_type for: %s.%s.%s" % (instance, db, table ))
                        else:
                            validate_check(registry[instance][db][table][check], check_type)



class CheckRepo(object):
    """ Maintains information about actual checks.

    """

    def __init__(self, check_dir):
        self.check_dir = check_dir
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
        assert check_type   in ('rule', 'profile', 'setup', 'teardown')
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
        """ Writes all check results at once to a sqlite database.
        #todo: check if this date already been tested, and if so, delete those prior results.
        #todo: add column to hold partitioning keys for incremental testing
        #todo: add "logical_delete" column for the deletes
        """
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
                                       self.results[instance][database][table][check]['violation_cnt'], ) )
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




class CheckRunner(object):

    def __init__(self, registry, check_repo, check_results):
        """ This is the general test runner class

        :param check_repo: CheckRepository object
        """
        self.repo     = check_repo
        self.registry = registry
        self.results  = check_results
        self.db_vars    = []
        self.table_vars = []

    def add_db_var(self, key, value):
        self.db_vars.append((key, value))
        os.environ[key] = value

    def drop_db_vars(self):
        for kv_tup in self.db_vars:
            os.environ.pop(kv_tup[0])

    def add_table_var(self, key, value):
        self.table_vars.append((key, value))
        os.environ[key] = value

    def drop_table_vars(self):
        for kv_tup in self.table_vars:
            os.environ.pop(kv_tup[0])
        self.table_vars = []

    def run_checks_for_tables(self, table_override):
        """ Runs checks on all tables, or just one if a non-None table value is provided.

        :return: None

        #todo: use table_override
        """
        for inst in self.registry.db_registry:
            for db in self.registry.db_registry[inst]:
                for table in self.registry.db_registry[inst][db]:

                    self.add_table_var('hapinsp_table', table)

                    # setup checks must happen first.  
                    # We can run multiple setup checks, but they'll be in arbitrary order.
                    for setup_check in [ x for x in self.registry.db_registry[inst][db][table]
                               if self.registry.db_registry[inst][db][table][x]['check_type'] == 'setup' ]:
                        reg_check = self.registry.db_registry[inst][db][table][setup_check]
                        if reg_check['check_status'] == 'active':
                            check_vars, rc = self._run_setup_check(reg_check)
                            check_status = 'active'
                        else:
                            check_vars = {}
                            rc         = None
                            check_status = reg_check['check_status']
                        for check_var in check_vars:
                            self.add_table_var(check_var, check_vars[check_var])
                        count = None
                        self.results.add(inst, db, table, setup_check, count, rc, check_status)


                    # checks run in arbitrary order - might want to sort them
                    for check in [ x for x in self.registry.db_registry[inst][db][table]
                                   if self.registry.db_registry[inst][db][table][x]['check_type']
                                       not in ('setup', 'teardown') ]:
                        reg_check = self.registry.db_registry[inst][db][table][check]
                        if reg_check['check_status'] == 'active':
                            count, rc = self._run_check(reg_check)
                            check_status = 'active'
                        else:
                            count = None
                            rc    = None
                            check_status = reg_check['check_status']
                        self.results.add(inst, db, table, check, count, rc, check_status)

                    self.drop_table_vars()


    def _run_setup_check(self, reg_check):
        check_fn       = self.repo.repo[reg_check['check_name']]['fqfn']
        raw_output, rc = self._run_check_file(check_fn)
        check_vars     = self.parse_setup_check_results(raw_output)
        return check_vars, rc

    def _run_check(self, reg_check):
        check_fn       = self.repo.repo[reg_check['check_name']]['fqfn']
        raw_output, rc = self._run_check_file(check_fn)
        violation_cnt  = self.parse_check_results(raw_output)
        return violation_cnt, rc

    def parse_check_results(self, raw_output):
        check = None
        count = 0
        #todo: make the following json
        for rec in raw_output:
            if 'violation_cnt:' in rec:
                fields = rec.split()
                count = int(fields[1])
                assert isnumeric(count)
        return count

    def parse_setup_check_results(self, raw_output):
        check = None
        print("raw_output: ")
        pp(raw_output)
        output = json.loads(raw_output)
        for key in output.keys():
            if not key.startswith('hapinsp_tablevar_'):
                raise ValueError("Invalid setup_check result - key has bad name: %s" % key)
        return output

    def _run_check_file(self, check_filename):
        assert isdir(self.repo.check_dir)
        assert isfile(pjoin(self.repo.check_dir, check_filename))
        check_fqfn = pjoin(self.repo.check_dir, check_filename)
        process = subprocess.Popen([check_fqfn], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.read()
        rc     = process.returncode
        #return output.decode().split('\n'), rc
        return output.decode(), rc







