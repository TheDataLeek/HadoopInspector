#!/usr/bin/env python2
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015, 2016 Will Farmer and Ken Farmer
"""

import os, sys, subprocess, datetime
import json, logging
from os.path import isdir, isfile, exists, dirname, basename
from os.path import join as pjoin
import errno
from pprint import pprint as pp

import hadoopinspector.core as core


class CheckRunner(object):

    def __init__(self, registry, check_repo, check_results, instance, database,
                 run_log_dir, log_level='debug', user_table_vars=None):
        """
        """
        assert isdir(run_log_dir)
        assert log_level in ('debug', 'info', 'warning', 'error', 'critical')
        self.repo     = check_repo
        self.registry = registry
        self.results  = check_results
        self.instance = instance
        self.database = database
        if not user_table_vars:
            self.user_table_vars = {}
        else:
            self.user_table_vars = {'hapinsp_tablecustom_%s' % key:var for (key, var) in user_table_vars.iteritems() }
        self.db_vars = []
        self.check_vars = []
        self.table_vars = []  # FIXME why list rather than dict?
        self.prior_table_vars = []
        self.run_log_dir = run_log_dir
        self.log_level = log_level
        self.check_file_handler = None
        self.check_logger = logging.getLogger('CheckLogger')
        self.run_logger = logging.getLogger('RunnerLogger')

    def _abort(self, msg):
        if self.check_logger:
            self.check_logger.critical(msg)
        else:
            print(msg)
        sys.exit(1)


    def both_logger(self, level, msg):

        if level == 'error':
            self.run_logger.error(msg)
            self.check_logger.error(msg)
        elif level == 'critical':
            self.run_logger.critical(msg)
            self.check_logger.critical(msg)
        else:
            self.run_logger.error(msg)
            self.check_logger.error(msg)


    def _get_logger(self, table, check):

        def mkdirs(path):
            try:
                os.makedirs(path)
            except OSError as exc:
                if exc.errno != errno.EEXIST or not os.path.isdir(path):
                    raise

        assert isdir(self.run_log_dir)
        check_log_dir = pjoin(self.run_log_dir, self.instance, self.database, table, check)
        mkdirs(check_log_dir)
        log_filename = pjoin(check_log_dir, 'check.log')

        #--- close any prior handler:
        if self.check_logger:
            self.check_logger.removeHandler(self.check_file_handler)
        if self.check_logger:
            if self.check_logger.handlers:
                self.check_logger.handlers[0].close()

        #--- create logger
        self.check_logger = logging.getLogger('CheckLogger')
        self.check_logger.setLevel(self.log_level.upper())

        #--- add formatting:
        log_format = '%(asctime)s : %(name)-12s : %(levelname)-8s : %(message)s'
        date_format = '%Y-%m-%d %H.%M.%S'
        check_formatter = logging.Formatter(log_format, date_format)

        #--- create rotating file handler
        self.check_file_handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000, backupCount=20)
        self.check_file_handler.setFormatter(check_formatter)
        self.check_logger.addHandler(self.check_file_handler)


    def add_db_var(self, key, value):
        if not key.startswith('hapinsp_'):
            self.run_logger.error("invalid table_var of: %s", key)
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
            self.run_logger.error("invalid table_var of: %s", key)
        else:
            value = '' if value is None or value.strip().lower() == 'none' else value
            self.table_vars.append((key, value))
            os.environ[key] = str(value)

    def get_table_var(self, key, default='nodefault'):
        for item in self.table_vars:
            if key == item[0]:
                return item[1]
        if default != 'nodefault':
            return default
        else:
            raise KeyError('%s not found' % key)

    def drop_table_vars(self):
        uniq_keys = { x[0] for x in self.table_vars }
        for key in uniq_keys:
            os.environ.pop(key)
        self.table_vars = []

    def add_prior_table_var(self, key, value):
        if not key.startswith('hapinsp_table'):
            self.run_logger.error("invalid table_var of: %s", key)
        else:
            value = '' if value is None or value.strip().lower() == 'none' else value
            if key.endswith('_prior'):
                adj_key = key
            else:
                adj_key = key + '_prior'
            self.run_logger.debug('add prior key: %s val: %s' % (adj_key, value))
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
            self.run_logger.error("Invalid checkcustom var: %s", key)
            return
        self.check_vars.append((key, value))
        os.environ[key] = (value)

    def drop_check_vars(self):
        uniq_keys = { x[0] for x in self.check_vars }
        for key in uniq_keys:
            os.environ.pop(key)
        self.check_vars = []


    def run_checks_for_tables(self):
        """ Runs checks on all tables, or just one if a non-None table value is provided.
        """
        for table in self.registry.registry:

            self.add_table_var('hapinsp_table', table)
            table_status = 'active'
            self.run_logger.debug('table: %s', table)

            #------  setup checks must happen first.   -----------------------------
            for setup_check in sorted([ x for x in self.registry.registry[table]
                                       if self.registry.registry[table][x]['check_type'] == 'setup' ]):
                reg_check = self.registry.registry[table][setup_check]
                if reg_check['check_status'] == 'active':
                    self._run_setup_check(table, setup_check, reg_check)

            #------  user table vars get set next - and may override check or other vars  ----------
            for key, val in self.user_table_vars.items():
                self.add_table_var(key, val)

            # bypass checks if setup marked this table inactive:
            if table_status == 'inactive':
                continue

            #------  regular checks (rules or profiles) can now run  -----------------------------
            for check in sorted([ x for x in self.registry.registry[table]
                                  if self.registry.registry[table][x]['check_type']
                                     not in ('setup', 'teardown') ]):
                reg_check = self.registry.registry[table][check]
                self._run_check(table, check, reg_check)

            self.drop_table_vars()

        self.results.write_to_sqlite()


    def _run_setup_check(self, table, setup_check, reg_check):

        start_iso8601ext = datetime.datetime.utcnow()
        # drop out if inactive:
        if reg_check['check_status'] == 'inactive':
            stop_iso8601ext = datetime.datetime.utcnow()
            self.results.add(table, setup_check,
                             check_status=reg_check['check_status'],
                             check_type='setup', setup_vars='',
                             run_start_timestamp=start_iso8601ext, run_stop_timestamp=stop_iso8601ext,
                             data_start_timestamp=None, data_stop_timestamp=None)
            return

        # configure logger:
        self._get_logger(table, setup_check)
        self.check_logger.info('check started')

        # write prior setup to env:
        prior_setup_vars_string = self.results.get_prior_setup_vars(table, setup_check)
        if prior_setup_vars_string:
            prior_setup_vars = SetupVars(prior_setup_vars_string, self.check_logger)
            for key, val in prior_setup_vars.tablecustom_vars.items():
                self.add_prior_table_var(key, val)
            self.add_prior_table_var('hapinsp_tablecustom_internal_rc', prior_setup_vars.internal_rc)
            self.add_prior_table_var('hapinsp_table_data_start_ts', prior_setup_vars.data_start_ts)
            self.add_prior_table_var('hapinsp_table_data_stop_ts', prior_setup_vars.data_stop_ts)

        # add envvars specific to this check from the registry
        for key, val in reg_check.items():
            if key.startswith('hapinsp_checkcustom_'):
                self.add_check_var(key, val)
        self.add_check_var('hapinsp_check_mode', reg_check['check_mode'])

        # execute the setup check
        try:
            check_fn = self.repo.repo[reg_check['check_name']]['fqfn']
        except KeyError:
            self.both_logger('critical', "registry check not found: %s" % reg_check['check_name'])
            sys.exit(1)
        raw_output, check_rc = self._run_check_file(check_fn)

        # parse & record the output:
        try:
            setup_vars = SetupVars(raw_output, self.check_logger)
        except ValueError as e:
            setup_vars   = SetupVars({}, self.check_logger)
            rc           = 201
            self.both_logger('error', "Failed setup_check: %s" % setup_check)
            self.check_logger.error("Error: JSON error: %s", e)
            self.check_logger.error("Error on parsing %s %s", setup_check, raw_output)
        else:
            self.run_logger.debug('table: %s, setup tablecustom results: %s', table, setup_vars.tablecustom_vars)
            rc = max(int(check_rc), int(setup_vars.internal_rc))
            for key, val in setup_vars.tablecustom_vars.items():
                self.add_table_var(key, val)
            self.add_table_var('hapinsp_table_mode', setup_vars.table_mode)
            self.add_table_var('hapinsp_table_data_start_ts', setup_vars.data_start_ts)
            self.add_table_var('hapinsp_table_data_stop_ts', setup_vars.data_stop_ts)

        count = None
        stop_iso8601ext = datetime.datetime.utcnow()
        saved_vars = setup_vars.tablecustom_vars
        saved_vars['data_start_ts'] = setup_vars.data_start_ts
        saved_vars['data_stop_ts'] = setup_vars.data_stop_ts
        self.results.add(table, setup_check, count,
                          rc, reg_check['check_status'],
                          check_mode=setup_vars.table_mode,
                          check_type='setup', setup_vars=saved_vars,
                          run_start_timestamp=start_iso8601ext, run_stop_timestamp=stop_iso8601ext,
                          data_start_timestamp=setup_vars.data_start_ts,
                          data_stop_timestamp=setup_vars.data_stop_ts)
        self.drop_prior_table_vars()


    def _run_check(self, table, check, reg_check):

        start_iso8601ext = datetime.datetime.utcnow()
        if reg_check['check_status'] == 'inactive':
            stop_iso8601ext = datetime.datetime.utcnow()
            self.results.add(table, check, check_status='inactive',
                             run_start_timestamp=start_iso8601ext, run_stop_timestamp=stop_iso8601ext,
                             data_start_timestamp=None, data_stop_timestamp=None)
            return

        # add envvars specific to this check from the registry
        for key, val in reg_check.items():
            if key.startswith('hapinsp_checkcustom_'):
                self.add_check_var(key, val)
        self.add_check_var('hapinsp_check_mode', reg_check['check_mode'])

        # configure logger:
        self._get_logger(table, check)
        self.check_logger.info('check started')

        try:
            check_fn           = self.repo.repo[reg_check['check_name']]['fqfn']
        except KeyError:
            self.both_logger('Error', 'registry check not found: %s' % reg_check['check_name'])
            sys.exit(1)
        raw_output, check_rc        = self._run_check_file(check_fn)

        try:
            check_vars   = CheckVars(raw_output, self.check_logger)
            actual_mode  = check_vars.mode
        except ValueError as e:
            count        = -1
            rc           = 202
            self.both_logger('ERROR', "Failed check: %s for table: %s" % (check, table))
            self.check_logger.error("Error: JSON error: %s", e)
            self.check_logger.error("Error on parsing  %s  %s", check_fn, raw_output)
            actual_mode  = None
        else:
            count       = check_vars.violation_cnt
            rc          = max(int(check_rc), int(check_vars.internal_rc))
            self.run_logger.debug('table: %s, check: %s, rc: %s, violations: %s, check log: %s', table, check, rc, count, check_vars.log)

        stop_iso8601ext = datetime.datetime.utcnow()
        if actual_mode == 'incremental':
            actual_data_start_iso8601 = self.get_table_var('hapinsp_table_data_start_ts', None)
            actual_data_stop_iso8601  = self.get_table_var('hapinsp_table_data_stop_ts', None)
        else:
            actual_data_start_iso8601 = None
            actual_data_stop_iso8601 = None
        self.results.add(table, check, count, rc, reg_check['check_status'],
                         check_mode=actual_mode, setup_vars=dict(self.check_vars + self.table_vars),
                         run_start_timestamp=start_iso8601ext,
                         run_stop_timestamp=stop_iso8601ext,
                         data_start_timestamp=actual_data_start_iso8601,
                         data_stop_timestamp=actual_data_stop_iso8601)

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

    def __init__(self, raw_output, check_logger):
        self.reserved_keys    = ['rc', 'violations', 'mode', 'log']
        self.raw_output       = raw_output
        self.internal_rc      = None
        self.violation_cnt    = None
        self._mode            = None
        self.log              = None
        self.check_logger     = check_logger
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
            self.check_logger.error("invalid check results: %s" % self.raw_output)
            if self.raw_output is None:
                self.check_logger.error("check results raw_output is None")
            raise

        for key, val in output_vars.items():
            if self._is_reserved_var(key):
                if key == 'rc':
                    self.internal_rc = val
                elif key == 'mode':
                    self.mode = val
                elif key == 'log':
                    self.check_logger.info(val)
                    self.log = val
                elif key == 'violations':
                    self.violation_cnt = val
                    if not core.isnumeric(val):
                        self.check_logger.error("invalid violations field")
                        self.violations_cnt = -1
            elif key.startswith('hapinsp_tablecustom_'):
                self.check_logger.debug('%s=%s', key, val)
            elif key.startswith('hapinsp_'):
                self.check_logger.debug('%s=%s', key, val)
            else:
                msg = "Invalid check result - key has bad name: %s" % key
                self.check_logger.error(msg)
                raise ValueError(msg)

        if self.violation_cnt is None:
            raise ValueError('invalid violation_cnt')
        elif self.internal_rc is None:
            raise ValueError('invalid internal_rc')



class SetupVars(object):

    def __init__(self, raw_output, check_logger):
        self.reserved_keys    = ['rc', 'table_status', 'mode', 'log', 'data_start_ts', 'data_stop_ts']
        self.raw_output       = raw_output
        self.tablecustom_vars = {}
        self.internal_rc      = -1
        self.table_status     = 'active'
        self.data_start_ts    = None
        self.data_stop_ts     = None
        self._table_mode      = None
        self.check_logger     = check_logger
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
        if self.raw_output is None:
            raise ValueError('setup output is None')
        elif isinstance(self.raw_output, str) and self.raw_output.strip() == '':
            raise ValueError('setup output is blank string')
        elif isinstance(self.raw_output, dict) and len(self.raw_output.keys()) == 0:
            return

        try:
            output_vars = json.loads(self.raw_output)
        except (TypeError, ValueError):
            self.check_logger.critical("Error: invalid setup check results: %s" % self.raw_output)
            if self.raw_output is None:
                self.check_logger.critical("Error: setup check results raw_output is None")
            raise

        self.internal_rc = None
        self.data_start_ts = None
        self.data_stop_ts = None
        self.table_status = 'active'
        for key, val in output_vars.items():
            if self._is_reserved_var(key):
                if key == 'rc':
                    self.internal_rc = val
                elif key == 'table_status' and val:
                    self.table_status = val
                elif key == 'log':
                    self.check_logger.info(val)
                elif key == 'mode' and val:
                    self.table_mode = val
                elif key == 'data_start_ts' and val:
                    if not core.valid_iso8601(val, 'basic'):
                        raise ValueError("Invalid setup_check result - data_start_ts has invalid value: %s" % val)
                    self.data_start_ts = val
                elif key == 'data_stop_ts' and val:
                    if not core.valid_iso8601(val, 'basic'):
                        raise ValueError("Invalid setup_check result - data_stop_ts has invalid value: %s" % val)
                    self.data_stop_ts = val
            elif key == 'data_start_timestamp':
                if not core.valid_iso8601(val, 'basic'):
                    raise ValueError("Invalid setup_check result - data_stop_ts has invalid value: %s" % val)
                self.data_start_ts = val
                self.check_logger.warning("deprecated data_start_timestamp found!")
            elif key == 'data_stop_timestamp':
                if not core.valid_iso8601(val, 'basic'):
                    raise ValueError("Invalid setup_check result - data_stop_ts has invalid value: %s" % val)
                self.data_stop_ts = val
                self.check_logger.warning("deprecated data_stop_timestamp found!")
            elif self._is_custom_var(key):
                self.tablecustom_vars[key] = val
                self.check_logger.debug('tablecustom_%s=%s', key, val)
            else:
                raise ValueError("Invalid setup_check result - key has bad name: %s" % key)


