#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""

import os, sys, time, datetime, subprocess
import json
from pprint import pprint as pp
import copy
import validictory
from os.path import isdir, isfile, exists, dirname, basename
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
                        "check_mode":   "full",
                        "check_scope":  "row",
                        "check_status": "active"
                        "hapinsp_checkcustom_cols": date_id
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
                  check_mode, check_scope, registry=None, **checkvars):
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
               'check_scope':   check_scope }
        for key in checkvars:
            if not key.startswith('hapinsp_checkcustom_'):
                print("Invalid registry checkvar: %s" % key)
                sys.exit(1)
            registry[instance][db][table][check][key] = checkvars[key]


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
                    "check_mode":   {"type": "null"},
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

    def __init__(self, db_fqfn=None):
        self.db_fqfn = db_fqfn
        self.start_dt = datetime.datetime.utcnow()
        self.results = {}
        self.setup_results = {}

    def add(self, instance, database, table, check, violations, rc,
            check_status=None,
            check_type='rule',
            check_policy_type='quality',
            check_mode='full',
            check_unit='rows',
            check_scope=-1,
            check_severity_score=-1,
            run_start_timestamp=None,
            run_stop_timestamp=None,
            setup_vars=None):
        assert isnumeric(rc)
        assert violations is None or isnumeric(violations), "Invalid violations: %s" % violations
        assert check_type   in ('rule', 'profile', 'setup', 'teardown')
        assert check_policy_type   in ('quality', 'consistency', 'data-management'), "invalid policy_type: %s" % check_policy_type
        assert check_unit   in ('rows', 'tables')
        assert check_mode   in ('full', 'incremental', 'auto', None), "invalid check_mode: %s" % check_mode
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

        self.results[instance][database][table][check]['violation_cnt']        = violations
        self.results[instance][database][table][check]['rc']                   = int(rc)
        self.results[instance][database][table][check]['check_status']         = check_status
        self.results[instance][database][table][check]['check_unit']           = check_unit
        self.results[instance][database][table][check]['check_type']           = check_type
        self.results[instance][database][table][check]['check_policy_type']    = check_type
        self.results[instance][database][table][check]['check_mode']           = '' if check_mode is None else check_mode
        self.results[instance][database][table][check]['check_scope']          = check_scope
        self.results[instance][database][table][check]['check_severity_score'] = check_severity_score
        self.results[instance][database][table][check]['run_start_timestamp']  = run_start_timestamp
        self.results[instance][database][table][check]['run_stop_timestamp']   = run_stop_timestamp
        self.results[instance][database][table][check]['setup_vars']           = '' if setup_vars is None else json.dumps(setup_vars)

    def get_max_rc(self):
        max_rc = 0
        for instance in self.results:
            for database in self.results[instance]:
                for table in self.results[instance][database]:
                    for check in self.results[instance][database][table]:
                        if self.results[instance][database][table][check]['rc'] > max_rc:
                            max_rc = self.results[instance][database][table][check]['rc']
        return max_rc

    def get_formatted_results(self, detail):
        def coalesce(val1, val2):
            if val1 is not None:
                return val1
            else:
                return val2

        formatted_results = []
        for inst in self.results:
            for db in self.results[inst]:
                for tab in sorted(self.results[inst][db]):
                    for setup_check in sorted({ x for x in self.results[inst][db][tab]
                                       if self.results[inst][db][tab][x]['check_type'] == 'setup' }):
                        if detail:
                            rec = '%s|%s|%s|%s|%s|%s' % (tab, setup_check,
                                        self.results[inst][db][tab][setup_check]['check_mode'],
                                        self.results[inst][db][tab][setup_check]['rc'],
                                        coalesce(self.results[inst][db][tab][setup_check]['violation_cnt'], ''),
                                        coalesce(self.results[inst][db][tab][setup_check]['setup_vars'], '') )
                        else:
                            rec = '%s|%s|%s|%s|%s' % (tab, setup_check,
                                        self.results[inst][db][tab][setup_check]['check_mode'],
                                        self.results[inst][db][tab][setup_check]['rc'],
                                        coalesce(self.results[inst][db][tab][setup_check]['violation_cnt'], '') )
                        formatted_results.append(rec)

                    for check in sorted({ x for x in self.results[inst][db][tab]
                                 if self.results[inst][db][tab][x]['check_type'] not in ('setup', 'teardown') }):
                        if detail:
                            rec = '%s|%s|%s|%s|%s|%s' % (tab, check,
                                        self.results[inst][db][tab][check]['check_mode'],
                                        self.results[inst][db][tab][check]['rc'],
                                        coalesce(self.results[inst][db][tab][check]['violation_cnt'], ''),
                                        coalesce(self.results[inst][db][tab][check]['setup_vars'], '') )
                        else:
                            rec = '%s|%s|%s|%s|%s' % (tab, check,
                                        self.results[inst][db][tab][check]['check_mode'],
                                        self.results[inst][db][tab][check]['rc'],
                                        coalesce(self.results[inst][db][tab][check]['violation_cnt'], '') )
                        formatted_results.append(rec)
        return formatted_results

    def write_to_sqlite(self):
        """ Writes all check results at once to a sqlite database.
        #todo: check if this date already been tested, and if so, delete those prior results.
        #todo: add column to hold partitioning keys for incremental testing
        #todo: add "logical_delete" column for the deletes
        """
        if self.db_fqfn is None:
            abort("Error: no sqlite file name provided to CheckResults")
        if not isfile(self.db_fqfn):
            print("warning: sqlitedb not found - will create database")
            create_sqlite_db(self.db_fqfn)

        conn = sqlite3.connect(self.db_fqfn)
        if not istable(conn, 'check_results'):
            print("warning: no check_results table found - will create database")
            create_sqlite_db(self.db_fqfn)

        stop_dt = datetime.datetime.utcnow()
        run_id  = 0
        cur  = conn.cursor()
        check_recs = []

        for inst in self.results:
            for db in self.results[inst]:
                for table in self.results[inst][db]:
                    for check in self.results[inst][db][table]:
                        check_recs.append( (inst, db, table, check,
                                self.results[inst][db][table][check]['check_type'],
                                self.results[inst][db][table][check]['check_policy_type'],
                                self.results[inst][db][table][check]['check_mode'],
                                self.results[inst][db][table][check]['check_unit'],
                                self.results[inst][db][table][check]['check_status'],
                                run_id,
                                (self.results[inst][db][table][check]['run_start_timestamp'] or self.start_dt),
                                (self.results[inst][db][table][check]['run_stop_timestamp']  or stop_dt),
                                self.results[inst][db][table][check]['rc'],
                                self.results[inst][db][table][check]['check_scope'],
                                self.results[inst][db][table][check]['check_severity_score'],
                                self.results[inst][db][table][check]['violation_cnt'],
                                self.results[inst][db][table][check]['setup_vars'] ) )

        if check_recs:
            check_sql  = """INSERT INTO check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  """
            cur.executemany(check_sql, check_recs)
            conn.commit()

        conn.close()


    def get_prior_setup_vars(self, inst, db, table, setup_check):
        sql  = ("SELECT env_vars "
                "FROM check_results  cr "
                "    INNER JOIN (SELECT MAX(run_start_timestamp) AS run_start_timestamp "
                "                 FROM check_results "
                "                 WHERE instance_name = '{inst}' "
                "                  AND database_name  = '{db}' "
                "                  AND table_name     = '{tb}' "
                "                  AND check_name     = '{cn}' ) as max_time "
                "       ON cr.run_start_timestamp = max_time.run_start_timestamp "
                "WHERE instance_name = '{inst}' "
                "  AND database_name = '{db}' "
                "  AND table_name    = '{tb}' "
                "  AND check_name    = '{cn}' "
                " LIMIT 1"
                ";" )
        conn = sqlite3.connect(self.db_fqfn)
        c    = conn.cursor()
        try:
            c.execute(sql.format(inst=inst, db=db, tb=table, cn=setup_check))
        except sqlite3.OperationalError as e:
            print("get_prior_setup_vars failed!")
            print(e)
        results = c.fetchall()
        conn.commit()
        conn.close()
        if results == []:
            return None
        else:
            return results[0][0]


def create_sqlite_db(db_fqfn):
    check_results_cmd = """ \
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
            run_id              INT,       \
            run_start_timestamp TIMESTAMP, \
            run_stop_timestamp  TIMESTAMP, \
            check_rc            INT,   \
            check_scope         INT,   \
            check_severity_score INT,  \
            check_violation_cnt INT,   \
            env_vars              ) """

    conn = sqlite3.connect(db_fqfn)
    c    = conn.cursor()
    c.execute(check_results_cmd)
    conn.commit()
    conn.close()
    #print("results.sqlite database successfully created")




class CheckRunner(object):

    def __init__(self, registry, check_repo, check_results, instance, database):
        """
        """
        self.repo     = check_repo
        self.registry = registry
        self.results  = check_results
        self.instance = instance
        self.database = database
        self.db_vars = []
        self.check_vars = []
        self.table_vars = []
        self.prior_table_vars = []

    def add_db_var(self, key, value):
        if not key.startswith('hapinsp_'):
            print("Error: invalid table_var of: %s" % key)
        else:
            self.db_vars.append((key, value))
            os.environ[key] = str(value)

    def drop_db_vars(self):
        uniq_keys = { x[0] for x in self.db_vars }
        for key in uniq_keys:
            os.environ.pop(key)
        self.db_vars = []

    def add_table_var(self, key, value):
        if not key.startswith('hapinsp_table'):
            print("Error: invalid table_var of: %s" % key)
        else:
            self.table_vars.append((key, value))
            os.environ[key] = str(value)

    def drop_table_vars(self):
        uniq_keys = { x[0] for x in self.table_vars }
        for key in uniq_keys:
            os.environ.pop(key)
        self.table_vars = []

    def add_prior_table_var(self, key, value):
        if not key.startswith('hapinsp_table'):
            print("Error: invalid table_var of: %s" % key)
        else:
            adj_key = key + '_prior'
            self.prior_table_vars.append((adj_key, value))
            os.environ[adj_key] = str(value)

    def drop_prior_table_vars(self):
        uniq_keys = { x[0] for x in self.prior_table_vars }
        for key in uniq_keys:
            os.environ.pop(key)
        self.prior_table_vars = []

    def add_check_var(self, key, value):
        if key == 'hapinsp_check_mode':
            pass
        elif not key.startswith('hapinsp_checkcustom_'):
            print("Invalid checkcustom var: %s" % key)
            return
        self.check_vars.append((key, value))
        os.environ[key] = (value)

    def drop_check_vars(self):
        uniq_keys = { x[0] for x in self.check_vars }
        for key in uniq_keys:
            os.environ.pop(key)
        self.check_vars = []


    def run_checks_for_tables(self, table_override):
        """ Runs checks on all tables, or just one if a non-None table value is provided.
        """
        inst = self.instance
        db   = self.database
        for table in self.registry.db_registry[inst][db]:

            self.add_table_var('hapinsp_table', table)
            table_status = 'active'

            #------  setup checks must happen first.   -----------------------------
            for setup_check in sorted([ x for x in self.registry.db_registry[inst][db][table]
                                       if self.registry.db_registry[inst][db][table][x]['check_type'] == 'setup' ]):
                reg_check = self.registry.db_registry[inst][db][table][setup_check]
                self._run_setup_check(table, setup_check, reg_check)

            # bypass checks if setup marked this table inactive:
            if table_status == 'inactive':
                continue

            # regular checks (could be either rules or prfiles)
            #------  regular checks (rules or profiles) can now run  -----------------------------
            for check in sorted([ x for x in self.registry.db_registry[inst][db][table]
                                  if self.registry.db_registry[inst][db][table][x]['check_type']
                                     not in ('setup', 'teardown') ]):
                reg_check = self.registry.db_registry[inst][db][table][check]
                self._run_check(table, check, reg_check)

            self.drop_table_vars()

        self.results.write_to_sqlite()


    def _run_setup_check(self, table, setup_check, reg_check):

        # drop out if inactive:
        if reg_check['check_status'] == 'inactive':
            self.results.add(self.instance, self.database, table, setup_check,
                             count=None, rc=None, check=reg_check['check_status'],
                             check_type='setup', setup_vars='')
            return

        # write prior setup to env:
        prior_setup_vars_string = self.results.get_prior_setup_vars(self.instance, self.database, table, setup_check)
        if prior_setup_vars_string:
            prior_setup_vars = SetupVars(prior_setup_vars_string)
            for key, val in prior_setup_vars.tablecustom_vars.items():
  
                self.add_prior_table_var(key, val)
            self.add_prior_table_var('hapinsp_tablecustom_internal_rc_prior', prior_setup_vars.internal_rc)

        # add envvars specific to this check from the registry
        for key, val in reg_check.items():
            if key.startswith('hapinsp_checkcustom_'):
                self.add_check_var(key, val)
        self.add_check_var('hapinsp_check_mode', reg_check['check_mode'])

        # execute the setup check
        try:
            check_fn           = self.repo.repo[reg_check['check_name']]['fqfn']
        except KeyError:
            print("Error: registry check not found: %s" % reg_check['check_name'])
            sys.exit(1)
        raw_output, check_rc        = self._run_check_file(check_fn)

        # parse & record the output:
        try:
            setup_vars = SetupVars(raw_output)
        except ValueError as e:
            setup_vars   = SetupVars({})
            table_status = 'inactive'
            rc           = 201
            print("Failed setup_check: %s" % setup_check)
            print("Error: JSON error: %s" % e)
            printerr("Error on parsing ", setup_check, raw_output)
        else:
            rc = setup_vars.internal_rc
            for key, val in setup_vars.tablecustom_vars.items():
                self.add_table_var(key, val)
            self.add_table_var('hapinsp_table_mode', setup_vars.table_mode)
            table_status = setup_vars.table_status

        count = None
        self.results.add(self.instance, self.database, table, setup_check, count,
                          rc, reg_check['check_status'],
                          check_mode=setup_vars.table_mode,
                          check_type='setup', setup_vars=setup_vars.tablecustom_vars)
        self.drop_prior_table_vars()


    def _run_check(self, table, check, reg_check):

        if reg_check['check_status'] == 'inactive':
            self.results.add(self.instance, self.database, table, check,
                             count=None, rc=None, check_status='inactive')
            return

        # add envvars specific to this check from the registry
        for key, val in reg_check.items():
            if key.startswith('hapinsp_checkcustom_'):
                self.add_check_var(key, val)
        self.add_check_var('hapinsp_check_mode', reg_check['check_mode'])

        try:
            check_fn           = self.repo.repo[reg_check['check_name']]['fqfn']
        except KeyError:
            print("Error: registry check not found: %s" % reg_check['check_name'])
            sys.exit(1)
        raw_output, check_rc        = self._run_check_file(check_fn)

        try:
            check_vars   = CheckVars(raw_output)
            actual_mode  = check_vars.mode
        except ValueError as e:
            count        = None
            int_rc       = None
            rc           = 202
            print("Failed check: %s" % check)
            print("Error: JSON error: %s" % e)
            printerr("Error on parsing ", check_fn, raw_output)
            actual_mode  = None
        else:
            count       = check_vars.violation_cnt
            rc          = max(int(check_rc), int(check_vars.internal_rc))

        self.results.add(self.instance, self.database, table, check,
                         count, rc, reg_check['check_status'],
                         check_mode=actual_mode,
                         setup_vars=dict(self.check_vars + self.table_vars))

        # remove any check-specific envvars:
        self.drop_check_vars()


    def _run_check_file(self, check_filename):
        assert isdir(self.repo.check_dir)
        assert isfile(pjoin(self.repo.check_dir, check_filename))
        check_fqfn = pjoin(self.repo.check_dir, check_filename)
        process = subprocess.Popen([check_fqfn], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.read()
        rc     = process.returncode
        return output.decode(), rc



class CheckVars(object):

    def __init__(self, raw_output):
        self.reserved_keys    = ['rc', 'violations', 'mode']
        self.raw_output       = raw_output
        self.internal_rc      = None
        self.violation_cnt    = None
        self._mode            = None
        self._parse_raw_output()

    @property
    def mode(self):
        if self._mode is None:
            return 'full'
        else:
            return self._mode
    @mode.setter
    def mode(self, value):
        self._mode = value

    def _is_reserved_var(self, key):
        if key in self.reserved_keys:
            return True
        else:
            return False

    def _parse_raw_output(self):
        try:
            output_vars = json.loads(self.raw_output)
        except (TypeError, ValueError):
            print("Error: invalid check results: %s" % self.raw_output)
            if self.raw_output is None:
                print("Error: check results raw_output is None")
            raise

        for key, val in output_vars.items():
            if self._is_reserved_var(key):
                if key == 'rc':
                    self.internal_rc = val
                elif key == 'mode':
                    self.mode = val
                elif key == 'violations':
                    self.violation_cnt = val
                    if not isnumeric(val):
                        print("Invalid violations field")
                        self.violations_cnt = -1
            else:
                raise ValueError("Invalid check result - key has bad name: %s" % key)



class SetupVars(object):

    def __init__(self, raw_output):
        self.reserved_keys    = ['rc', 'table_status', 'mode']
        self.raw_output       = raw_output
        self.tablecustom_vars = {}
        self.internal_rc      = -1
        self.table_status     = 'active'
        self._table_mode      = None
        self._parse_raw_output()

    @property
    def table_mode(self):
        if self._table_mode is None:
            return 'auto'
        else:
            return self._table_mode
    @table_mode.setter
    def table_mode(self, value):
        self._table_mode = value


    def _is_reserved_var(self, key):
        if key in self.reserved_keys:
            return True
        else:
            return False

    def _is_custom_var(self, key):
        if key.startswith('hapinsp_tablecustom_'):
            return True
        else:
            return False

    def _parse_raw_output(self):
        try:
            output_vars = json.loads(self.raw_output)
        except (TypeError, ValueError):
            print("Error: invalid setup check results: %s" % self.raw_output)
            if self.raw_output is None:
                print("Error: setup check results raw_output is None")
            raise

        internal_rc = None
        table_status = 'active'
        for key, val in output_vars.items():
            if self._is_reserved_var(key):
                if key == 'rc':
                    self.internal_rc = val
                elif key == 'table_status' and val:
                    self.table_status = val
                elif key == 'mode' and val:
                    self.table_mode = val
            elif self._is_custom_var(key):
                self.tablecustom_vars[key] = val
            else:
                raise ValueError("Invalid setup_check result - key has bad name: %s" % key)





def istable(dbcon, tablename):
    cur = dbcon.cursor()
    cur.execute("select name from sqlite_master where type='table'")
    results = cur.fetchall()
    cur.close()
    if not results:
        return False
    else:
        if tablename in results[0]:
            return True
        else:
            return False


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


def printerr(*args):
    sys.stderr.write(' '.join(map(str,args)) + '\n')





