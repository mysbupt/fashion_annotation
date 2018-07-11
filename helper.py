#!/usr/bin/env python

import io
import time
import datetime
import yaml
import MySQLdb
import simplejson as json
from StringIO import StringIO

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import SimpleStatement
from cassandra.query import ValueSequence


def create_connection_cassandra(keyspace):
    hostname = '127.0.0.1'
    nodes = [hostname]
    cluster = Cluster(nodes)
    session = cluster.connect(keyspace)
    return session


def create_connection_mysql(db_name):
    config = yaml.load(open('./config.yaml'))
    HOSTNAME = config['Mysql']['HOSTNAME']
    USERNAME = config['Mysql']['USERNAME']
    PASSWD = config['Mysql']['PASSWD']
    try:
        conn = MySQLdb.connect(host=HOSTNAME, user=USERNAME, passwd=PASSWD , db=db_name)
        return conn
    except:
        print("connection to mysql error")
        exit()


def get_a_batch_of_data(conn_mysql, filters):
    batch = int(filters['batch'])
    last_index = filters['last_index']

    if batch > 100:
        batch = 100
    elif batch <= 0:
        batch = 50

    time_lb = filters['time'][0]
    time_up = filters['time'][1]

    loc_name = filters['location']['name']
    loc_lat_range = filters['location']['latitude']
    loc_lon_range = filters['location']['longitude']

    persons_range = filters['persons']
    faces_range = filters['faces']
    likes_range = filters['likes']
    comments_range = filters['comments']

    bloggers_list = filters['bloggers']
    hashtags_list = filters['hashtags']

    mysql_c = conn_mysql.cursor()
    mysql_c.execute("SELECT id FROM images LIMIT %s, %s", (last_index, batch , ))
    res = mysql_c.fetchall()
    images = [{'src': '/images/' + i[0] + '.jpg'} for i in res]
    return images


def get_an_image(conn_cassandra, image_id):
    CQL_str = "SELECT image FROM images WHERE img_url_md5 = %s"
    rows = conn_cassandra.execute(CQL_str, [image_id])
    
    return rows[0] 


def get_a_candidate(conn, username):
    c = conn.cursor()

    c.execute("SELECT * FROM products WHERE (labelling_or_not=? AND label_or_not=? AND label_person=?) OR (labelling_or_not=? AND label_or_not=?) limit 1", ('true', 'false', username, 'false', 'false'))
    res = c.fetchone()
    img_id = res[0]

    c.execute("UPDATE products SET labelling_or_not=?, label_person=? WHERE main_img_id=?", ('true', img_id, username))

    conn.commit()
    return res


def get_stats(conn):
    c = conn.cursor()

    res = {}

    c.execute("SELECT label_person, COUNT(*) FROM products WHERE label_or_not='true' GROUP BY label_person")
    res_all_cnt = c.fetchall()
    c.execute("SELECT label_person, COUNT(*) FROM products WHERE label_or_not='true' AND label='true' GROUP BY label_person")
    res_pos_cnt = c.fetchall()
    c.execute("SELECT label_person, COUNT(*) FROM products WHERE label_or_not='true' AND label='false' GROUP BY label_person")
    res_neg_cnt = c.fetchall()
    c.execute("SELECT name FROM users")
    users = c.fetchall()
    
    for user in users:
        res[user[0]] = {'all_cnt': 0, 'pos_cnt': 0, 'neg_cnt': 0}
    res['none'] = {'all_cnt': 0, 'pos_cnt': 0, 'neg_cnt': 0}
    for user, all_cnt in res_all_cnt:
        res[user]['all_cnt'] = all_cnt
    for user, pos_cnt in res_pos_cnt:
        res[user]['pos_cnt'] = pos_cnt
    for user, neg_cnt in res_neg_cnt:
        res[user]['neg_cnt'] = neg_cnt

    return res


def accept(conn, accept_id, username):
    c = conn.cursor()
    c.execute("UPDATE products SET labelling_or_not=?, label_or_not=?, label_person=?, label_time=?, label=? WHERE main_img_id=?", ('false', 'true', username, str(time.time()), 'true', accept_id))
    c.execute("UPDATE users SET times = times + 1 WHERE name=?", (username, ))

    conn.commit()


def reject(conn, reject_id, username):
    c = conn.cursor()
    c.execute("UPDATE products SET labelling_or_not=?, label_or_not=?, label_person=?, label_time=?, label=? WHERE main_img_id=?", ('false', 'true', username, str(time.time()), 'false', reject_id))
    c.execute("UPDATE users SET times = times + 1 WHERE name=?", (username, ))

    conn.commit()


def convert_dbres_dict(next_matching):
    next_matching_dict = {}
    next_matching_dict['main_img_url_md5'] = next_matching[0]
    next_matching_dict['main_img_url'] = next_matching[1]
    products = []
    for each_prd_img in next_matching[2].split('|'):
        products.append({'img_url_md5': each_prd_img})
    next_matching_dict['products'] = products

    return next_matching_dict


def authenticate(conn, username, passwd):
    c = conn.cursor()
    c.execute("SELECT * FROM annotator WHERE username=%s AND passwd=%s", (username, passwd))
    res = c.fetchall()
    if len(res) == 1:
        return True
    else:
        return False
