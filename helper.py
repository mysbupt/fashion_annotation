#!/usr/bin/env python

import io
import time
import datetime
import MySQLdb
import simplejson as json
from StringIO import StringIO

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import SimpleStatement
from cassandra.query import ValueSequence


total_cnt_cache = {}


def create_connection_cassandra(keyspace):
    hostname = '127.0.0.1'
    nodes = [hostname]
    cluster = Cluster(nodes)
    session = cluster.connect(keyspace)
    return session


def create_connection_mysql(db_name):
    HOSTNAME = 'localhost'
    USERNAME = 'root'
    PASSWD = 'mayunshan880909'
    try:
        conn = MySQLdb.connect(host=HOSTNAME, user=USERNAME, passwd=PASSWD , db=db_name)
        return conn
    except:
        print("connection to mysql error")
        exit()


def check_req(req):
    print req
    filters = req['filters']

    query = {}
    if 'time' in filters.keys() and filters['time'][0] != '' and filters['time'][0] is not None and filters['time'][1] != '' and filters['time'][1] is not None:
        query['publish_time'] = "publish_time BETWEEN '%s' AND '%s'" %(filters['time'][0], filters['time'][1])

    if 'location' in filters.keys():
        if 'latitude' in filters['location'].keys() and filters['location']['latitude'][0] != '' and filters['location']['latitude'][0] is not None and filters['location']['latitude'][1] != '' and filters['location']['latitude'][1] is not None:
            query['location_latitude'] = "location_latitude BETWEEN '%s' AND '%s'" %(filters['location']['latitude'][0], filters['location']['latitude'][1])

        if 'longitude' in filters['location'].keys() and filters['location']['longitude'][0] != '' and filters['location']['longitude'][0] is not None and filters['location']['longitude'][1] != '' and filters['location']['longitude'][1] is not None:
            query['location_longitude'] = "location_longitude BETWEEN '%s' AND '%s'" %(filters['location']['longitude'][0], filters['location']['longitude'][1])

        if 'name' in filters['location'].keys() and filters['location']['name'] != '' and filters['location']['name'] is not None:
            query['parse_cc'] = "parse_cc = '%s'" %(filters['location']['name'])

    if 'persons' in filters.keys() and filters['persons'][0] != '' and filters['persons'][0] is not None and filters['persons'][1] != '' and filters['persons'][1] is not None:
        query['person_cnt'] = "num_of_person BETWEEN %s AND %s" %(filters['persons'][0], filters['persons'][1])

    if 'faces' in filters.keys() and filters['faces'][0] != '' and filters['faces'][0] is not None and filters['faces'][1] != '' and filters['faces'][1] is not None:
        query['faces_cnt'] = "num_of_face BETWEEN %s AND %s" %(filters['faces'][0], filters['faces'][1])

    if 'likes' in filters.keys() and filters['likes'][0] != '' and filters['likes'][0] is not None and filters['likes'][1] != '' and filters['likes'][1] is not None:
        query['likes'] = "likes BETWEEN %s AND %s" %(filters['likes'][0], filters['likes'][1])

    if 'comments' in filters.keys() and filters['comments'][0] != '' and filters['comments'][0] is not None and filters['comments'][1] != '' and filters['comments'][1] is not None:
        query['comments'] = "comments BETWEEN %s AND %s" %(filters['comments'][0], filters['comments'][1])

    if 'bloggers' in filters.keys() and filters['bloggers'] != [''] and filters['bloggers'] is not None:
        tmp_str = ','.join(["'" + i + "'" for i in filters['bloggers']])
        query['blogger'] = "blogger IN (%s)" %(filters['bloggers'])

    if 'hashtags' in filters.keys() and filters['hashtags'] != [''] and filters['hashtags'] is not None:
        tmp_str = ','.join(["'" + i + "'" for i in filters['hashtags']])
        query['tag'] = "tag IN (%s)" %(tmp_str)

    page_num = int(req['page']['page'])
    if page_num < 1:
        page_num = 1
    batch = int(req['page']['batch'])
    if batch > 1000:
        batch = 1000
    limit = "LIMIT %s, %s" %((page_num - 1) * batch, batch) 

    page_info = {"page": page_num, "batch": batch}
    print "query is:", query

    return query, limit, page_info


def get_total_cnt(conn_mysql, q_total_cnt):
    global total_cnt_cache
    print total_cnt_cache
    if q_total_cnt in total_cnt_cache:
        return total_cnt_cache[q_total_cnt] 
    else:
        mysql_c = conn_mysql.cursor()
        try:
            mysql_c.execute(q_total_cnt)
        except:
            print q_total_cnt
            exit()
        res = mysql_c.fetchall()
        total_cnt = res[0][0]
        total_cnt_cache[q_total_cnt] = total_cnt
        return total_cnt


def get_a_batch_of_data(conn_mysql, query, limit, page_info):
    mysql_c = conn_mysql.cursor()

    if len(query) != 0:
        filter_cnd = "WHERE " + " AND ".join(["(" + i + ")" for i in query.values()])
    else:
        filter_cnd = ""

    q_total_cnt = "SELECT count(*) FROM images " + filter_cnd
    total_cnt = get_total_cnt(conn_mysql, q_total_cnt)
    page = {'count': total_cnt, "totalPage": total_cnt / page_info['batch'], "limit": page_info['batch'], "page": page_info['page']}

    q_ret = "SELECT * FROM images " + filter_cnd + " " + limit

    print q_ret

    try:
        mysql_c.execute(q_ret)
    except:
        print q_ret
        exit()
    res = mysql_c.fetchall()
    res_data = []
    for i in res:
        data = {
            "id": i[0],
            "hash_tag": i[1],
            "src": '/images/' + i[0] + '.jpg',
            "href": i[3],
            "text": "",
            "blogger": i[7],
            "likes": i[8],
            "src_site": i[9],
            "object_detction": "",
            "face_detction": "",
            "location_name": "",
            "location_url": i[13],
            "comments": [14],
            "publish_time": "",
            "location_lat": i[16],
            "location_lon": i[17]
        }
        if i[11] != "null":
            data['face_detction'] = i[11]
        if i[10] != "null":
            data['object_detction'] = i[10]
        if i[13] != "null":
            data['location_url'] = i[13]
        try:
            data['text'] = i[4].decode('latin-1')
        except:
            pass
        try:
            data['location_name'] = i[12].decode('latin-1')
        except:
            pass
        try:
            data['publish_time'] = i[15].strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass

        res_data.append(data)

    return {'pages': page, 'data': res_data}


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
