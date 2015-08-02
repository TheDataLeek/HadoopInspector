#!/usr/bin/env python3

import sys
import json
import sqlite3
from flask import Flask, render_template
import falcon


app = Flask(__name__)
config = json.loads(open('config.json').read())

print(config)

connection = sqlite3.connect(config["db"])
cursor = connection.cursor()


@app.route('/')
def root():

    return {}


#@app.route('/api')
#def


if __name__ == '__main__':
    app.run(host='localhost',
            port=config['port'])
