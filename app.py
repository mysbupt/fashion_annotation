#!/usr/bin/env python

import io
import re
import time
import datetime
import hashlib
import simplejson as json
from flask import Flask, send_file, jsonify, flash, redirect, render_template, request, session, abort, url_for
from StringIO import StringIO
from helper import *


app = Flask(__name__)
conn_mysql = create_connection_mysql('fashion')
conn_cassandra = create_connection_cassandra('instagram_scene')


@app.route('/')
@app.route('/explore', methods=['GET'])
def explore():
    if 'username' in session:
        username = session['username']
        return render_template('explore.html', username=username)
    else:
        return redirect(url_for('login'))


@app.route('/get_items', methods=['POST'])
def get_items():
    req = request.get_json()
    print req
    filters = { 
        'time': [ '2017-01-01 00:00:00', '2018-07-04 00:00:00'], 
        'location': { 'name': "American", 'longitude': [ 0.0, 180.0], 'latitude': [ 0.0, 60.0 ] },
        'hashtags': [ 'winterwedding', 'summerwedding' ],
        'persons': [ 0, 10 ],
        'faces': [ 0, 10 ],
        'likes': [ 0, 10 ],
        'comments': [ 0, 10 ],
        'bloggers': [ 'svadbavdominicane' ],
        'last_index': req['last_index'],
        'batch': req['batch']
    }
    batch_of_data = get_a_batch_of_data(conn_mysql, filters)
    return jsonify(batch_of_data)


@app.route('/images/<image_id>.jpg', methods=['GET'])
def get_image(image_id):
    if re.match(r"\w{32}", image_id):
        image = get_an_image(conn_cassandra, image_id)
        return send_file(
            io.BytesIO(image[0]),
            mimetype='image/jpeg'
        )


@app.route('/login', methods=['GET', 'POST'])
def login():
    m = hashlib.md5()
    if request.method == 'POST':
        username = request.form['username']
        passwd = request.form['passwd']
        m.update(passwd)
        passwd = m.hexdigest()
        if not authenticate(conn_mysql, username, passwd):
            return render_template('login.html')
        else:
            session['username'] = request.form['username']
            return redirect(url_for('explore')) 
    else:
        return render_template('login.html')


@app.route('/logout', methods=['GET'])
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))


@app.route('/accept', methods=['POST'])
def accept_and_next():
    if 'username' in session:
        username = session['username']
        accept_id = request.form['accept_id']
        accept(conn, accept_id, username)

        next_matching = get_a_candidate(conn, username)
        return render_template('show_label.html', content=convert_dbres_dict(next_matching), username=username)
    else:
        return redirect(url_for('login'))


@app.route('/reject', methods=['POST'])
def reject_and_next():
    if 'username' in session:
        username = session['username']
        reject_id = request.form['reject_id']
        reject(conn, reject_id, username)

        next_matching = get_a_candidate(conn, username)
        return render_template('show_label.html', content=convert_dbres_dict(next_matching), username=username)
    else:
        return redirect(url_for('login'))


@app.route('/stats', methods=['GET'])
def stats():
    if 'username' in session:
        username = session['username']
        return render_template('show_stats.html', content=get_stats(conn), username=username)
    else:
        return redirect(url_for('login'))


if __name__ == "__main__":
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host='0.0.0.0', port=9090)
