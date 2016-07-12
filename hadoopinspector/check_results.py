#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015,2016 Will Farmer and Ken Farmer
"""

import os, sys, datetime
import json, logging
from os.path import isdir, isfile, exists, dirname, basename
from os.path import join as pjoin
from pprint import pprint as pp

import sqlite3

import hadoopinspector.core as core


class CheckResults(object):

    def __init__(self, inst, db, db_fqfn=None):
        self.inst    = inst
        self.db      = db
        self.db_fqfn = db_fqfn
        self.start_dt = datetime.datetime.utcnow()
        self.results = {}
        self.setup_results = {}
        self.logger = logging.getLogger('RunnerLogger')

        #--- create database & table if necessary:
        if not isfile(self.db_fqfn):
            self.logger.info("warning: sqlitedb not found - will create database")
            create_sqlite_db(self.db_fqfn)
        conn = sqlite3.connect(self.db_fqfn)
        if not istable(conn, 'check_results'):
            self.logger.info("warning: no check_results table found - will create database")
            create_sqlite_db(self.db_fqfn)

    def _abort(self, msg):
        if self.logger:
            self.logger.critical(msg)
        else:
            print(msg)
        sys.exit(1)

    def add(self, table, check, violations=-1, rc=-1,
            check_status='active',
            check_type='rule',
            check_policy_type='quality',
            check_mode='full',
            check_unit='rows',
            check_scope=-1,
            check_severity_score=-1,
            run_start_timestamp=None,
            run_stop_timestamp=None,
            data_start_timestamp=None,
            data_stop_timestamp=None,
            setup_vars=None):
        assert core.isnumeric(rc)
        assert violations is None or core.isnumeric(violations), "Invalid violations: %s" % violations
        assert check_type   in ('rule', 'profile', 'setup', 'teardown')
        assert check_policy_type   in ('quality', 'consistency', 'data-management'), "invalid policy_type: %s" % check_policy_type
        assert check_unit   in ('rows', 'tables')
        assert check_mode   in ('full', 'incremental', 'auto', None), "invalid check_mode: %s" % check_mode
        assert check_status in (None, 'active', 'inactive')
        assert core.isnumeric(check_scope)
        assert core.isnumeric(check_severity_score)
        assert isinstance(run_start_timestamp, datetime.datetime)
        assert isinstance(run_stop_timestamp, datetime.datetime)

        if table not in self.results:
            self.results[table] = {}
        if check not in self.results[table]:
            self.results[table][check] = {}

        self.results[table][check]['violation_cnt']        = violations
        self.results[table][check]['rc']                   = int(rc)
        self.results[table][check]['check_status']         = check_status
        self.results[table][check]['check_unit']           = check_unit
        self.results[table][check]['check_type']           = check_type
        self.results[table][check]['check_policy_type']    = check_type
        self.results[table][check]['check_mode']           = '' if check_mode is None else check_mode
        self.results[table][check]['check_scope']          = check_scope
        self.results[table][check]['check_severity_score'] = check_severity_score
        self.results[table][check]['run_start_timestamp']  = run_start_timestamp
        self.results[table][check]['run_stop_timestamp']   = run_stop_timestamp
        self.results[table][check]['data_start_timestamp'] = data_start_timestamp
        self.results[table][check]['data_stop_timestamp']  = data_stop_timestamp
        self.results[table][check]['setup_vars']           = '' if setup_vars is None else json.dumps(setup_vars)

    def get_max_rc(self):
        max_rc = 0
        for table in self.results:
            for check in self.results[table]:
                if self.results[table][check]['rc'] > max_rc:
                    max_rc = self.results[table][check]['rc']
        return max_rc

    def get_formatted_results(self, detail):
        assert detail in (True, False)
        def coalesce(val1, val2):
            if val1 is not None:
                return val1
            else:
                return val2

        formatted_results = []
        for tab in sorted(self.results):
            for setup_check in sorted({ x for x in self.results[tab]
                                if self.results[tab][x]['check_type'] == 'setup' }):
                if detail:
                    rec = '%s|%s|%s|%s|%s|%s|%s|%s' % (tab, setup_check,
                                self.results[tab][setup_check]['check_mode'],
                                self.results[tab][setup_check]['rc'],
                                coalesce(self.results[tab][setup_check]['violation_cnt'], ''),
                                coalesce(self.results[tab][setup_check]['setup_vars'], ''),
                                self.results[tab][setup_check]['data_start_timestamp'],
                                self.results[tab][setup_check]['data_stop_timestamp'] )
                else:
                    rec = '%s|%s|%s|%s|%s' % (tab, setup_check,
                                self.results[tab][setup_check]['check_mode'],
                                self.results[tab][setup_check]['rc'],
                                coalesce(self.results[tab][setup_check]['violation_cnt'], '') )
                formatted_results.append(rec)

            for check in sorted({ x for x in self.results[tab]
                            if self.results[tab][x]['check_type'] not in ('setup', 'teardown') }):
                if detail:
                    rec = '%s|%s|%s|%s|%s|%s|%s|%s' % (tab, check,
                                self.results[tab][check]['check_mode'],
                                self.results[tab][check]['rc'],
                                coalesce(self.results[tab][check]['violation_cnt'], ''),
                                coalesce(self.results[tab][check]['setup_vars'], ''),
                                self.results[tab][check]['data_start_timestamp'],
                                self.results[tab][check]['data_stop_timestamp'] )
                else:
                    rec = '%s|%s|%s|%s|%s' % (tab, check,
                                self.results[tab][check]['check_mode'],
                                self.results[tab][check]['rc'],
                                coalesce(self.results[tab][check]['violation_cnt'], '') )
                formatted_results.append(rec)
        return formatted_results

    def write_to_sqlite(self):
        """ Writes all check results at once to a sqlite database.
        #todo: check if this date already been tested, and if so, delete those prior results.
        #todo: add column to hold partitioning keys for incremental testing
        #todo: add "logical_delete" column for the deletes
        """
        conn = sqlite3.connect(self.db_fqfn)
        stop_dt = datetime.datetime.utcnow()
        run_id  = 0
        cur  = conn.cursor()
        check_recs = []

        for table in self.results:
            for check in self.results[table]:
                check_fields = self.results[table][check]
                check_recs.append( (self.inst, self.db, table, check,
                        check_fields['check_type'],
                        check_fields['check_policy_type'],
                        check_fields['check_mode'],
                        check_fields['check_unit'],
                        check_fields['check_status'],
                        run_id,
                        (check_fields['run_start_timestamp'] or self.start_dt),
                        (check_fields['run_stop_timestamp']  or stop_dt),
                        (check_fields['data_start_timestamp'] or check_fields['run_start_timestamp'] or self.start_dt),
                        (check_fields['data_stop_timestamp']  or check_fields['run_stop_timestamp'] or stop_dt),
                        check_fields['rc'],
                        check_fields['check_scope'],
                        check_fields['check_severity_score'],
                        check_fields['violation_cnt'],
                        check_fields['setup_vars'] ) )

        if check_recs:
            check_sql  = """INSERT INTO check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  """
            cur.executemany(check_sql, check_recs)
            conn.commit()

        conn.close()

    def get_prior_setup_vars(self, table, setup_check):
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
            c.execute(sql.format(inst=self.inst, db=self.db, tb=table, cn=setup_check))
        except sqlite3.OperationalError as e:
            self.logger.critical("get_prior_setup_vars failed!")
            self.logger.critical(e)
            self._abort("get_prior_setup_vars failed!")
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
            data_start_timestamp TIMESTAMP, \
            data_stop_timestamp  TIMESTAMP, \
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


