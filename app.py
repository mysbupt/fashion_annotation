#!/usr/bin/env python

import io
import re
import time
import datetime
import hashlib
import simplejson as json
from flask import Flask, send_from_directory, send_file, jsonify, flash, redirect, render_template, request, session, abort, url_for
from StringIO import StringIO
from helper import *

import yaml


app = Flask(__name__)
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'

conf = yaml.load(open("./config.yaml"))
conn_mysql = create_connection_mysql(conf["mysql"])
conn_cassandra = create_connection_cassandra(conf["cassandra"])

country_list = get_country_list_bydb(conn_mysql)
hashtag_list = get_hashtag_list_bydb(conn_mysql)
task_list = get_task_list_byfile()
cat_attr_val_list = get_cat_attr_val_list_byfile()
occasion_tag_mapping = yaml.load(open("./data/occasion_tag_rough_map.yaml"))

@app.route('/')
@app.route('/explore', methods=['GET'])
def explore():
    if 'username' in session and 'role' in session:
        username = session['username']
        role = session['role']
        #return render_template('explore.html', username=username)
        if role == "admin":
            return send_from_directory("./templates", "explore.html")
        elif role == "annotator":
            return send_from_directory("./templates", "annotate.html")
        else:
            return send_from_directory("./templates", "explore.html")
    else:
        return redirect(url_for('login'))


@app.route('/get_items', methods=['POST'])
def get_items():
    req = request.get_json()
    query, limit, page_info = check_req(req, occasion_tag_mapping, session["role"], session["username"])
    batch_of_data = get_a_batch_of_data(conn_mysql, query, limit, page_info)
    return jsonify(batch_of_data)


@app.route('/get_task_list', methods=['GET'])
def get_task_list():
    global task_list 
    return jsonify(task_list)


@app.route('/get_country_list', methods=['GET'])
def get_country_list():
    global country_list
    return jsonify(country_list)


@app.route('/get_cat_attr_val_list', methods=['GET'])
def get_cat_attr_val_list():
    global cat_attr_val_list
    return jsonify(cat_attr_val_list)


@app.route('/get_hashtag_list', methods=['GET'])
def get_hashtag_list():
    global hashtag_list
    return jsonify(hashtag_list)


@app.route('/label', methods=['POST'])
def label_specific_task():
    if 'username' in session:
        username = session['username']
        req = request.get_json()
        print "label request:", req
        res = label_by_req(conn_mysql, req)
        #{"msg": "success"}
        return jsonify(res)
    else:
        return redirect(url_for('login'))


@app.route('/annotate_clothes', methods=['POST'])
def label_clothes():
    if 'username' in session:
        username = session['username']
        req = request.get_json()
        print("%s annotate" %(username))
        res = annotate_clothes_by_req(conn_mysql, req, username)
        return jsonify(res)
    else:
        return redirect(url_for('login'))


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
        res, role = authenticate(conn_mysql, username, passwd)
        if not res: 
            return render_template('login.html')
        else:
            session['username'] = request.form['username']
            session['role'] = role
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
    app.run(host='0.0.0.0', port=2223, threaded=True)
