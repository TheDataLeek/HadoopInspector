#!/usr/bin/env python3

import sys
import json
import pandas
import datetime
from flask import Flask, render_template, Markup
import matplotlib.pyplot as plt
import mpld3


app = Flask(__name__)
config = json.loads(open('../config/config.json').read())

data = pandas.read_csv('/tmp/inspector_demo.csv',
                        parse_dates=['run_start_timestamp', 'run_check_start_timestamp', 'run_check_end_timestamp'],
                        date_parser=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S"))

def main():
    app.run(host='localhost',
            port=config['port'],
            debug=True)


#TODO: Clean up aggregation code and bin
@app.route('/')
def root():
    instances = data['instance_name'].unique()
    rules, rimages = time_series_data('run_check_violation_cnt', 'instance_name', data)
    checks, cimages = time_series_data('run_check_anomaly_score', 'instance_name', data)

    table = [[instances[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(instances))]

    content = render_template('index.html', name='Hadoop QA', table=table)
    return content


def time_series_data(tkey, gkey, dframe):
    scoresum = [group[tkey].sum() for key, group in dframe.groupby(gkey)]
    # Returns an array of pandas TimeSeries
    history_raw = [pandas.Series(df[tkey].values, index=df['run_check_end_timestamp'].values)
                        for df in [group[['run_check_end_timestamp', tkey]]
                                   for key, group in dframe.groupby(gkey)]]
    # Resample each timeseries by minute
    history = [hist.resample('T', how='count') for hist in history_raw]
    # Create each image
    images = []
    for hist in history:
        fig = plt.figure(figsize=(5, 1))
        ax = hist.plot()
        fig.suptitle(tkey)
        images.append(Markup(mpld3.fig_to_html(fig)))
    return scoresum, images


@app.route('/inspect/<instance>')
def instance(instance):
    dframe = data.loc[data['instance_name'] == instance]
    dbs = dframe['database_name'].unique()

    rules, rimages = time_series_data('run_check_violation_cnt', 'database_name', dframe)
    checks, cimages = time_series_data('run_check_anomaly_score', 'database_name', dframe)

    table = [[dbs[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(dbs))]

    content = render_template('tabular.html',
                              name=instance,
                              table=table)
    return content


@app.route('/inspect/<instance>/<database>')
def database(instance, database):
    dframe = data.loc[(data['instance_name'] == instance) & (data['database_name'] == database)]
    tables = dframe['table_name'].unique()

    rules, rimages = time_series_data('run_check_violation_cnt', 'table_name', dframe)
    checks, cimages = time_series_data('run_check_anomaly_score', 'table_name', dframe)

    table = [[tables[i], rules[i], rimages[i], checks[i], cimages[i]] for i in range(len(tables))]

    content = render_template('tabular.html',
                              name=instance + '/' + database,
                              table=table)
    return content


@app.route('/inspect/<instance>/<database>/<table>')
def table(instance, database, table):
    dframe = data.loc[(data['instance_name'] == instance) &
                      (data['database_name'] == database) &
                      (data['table_name'] == table)]
    content = render_template('details.html',
                              instance=instance,
                              database=database,
                              table=table,
                              dframe=str(dframe))
    return content


if __name__ == '__main__':
    sys.exit(main())
