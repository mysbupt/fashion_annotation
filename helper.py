#!/usr/bin/env python

import io
import time
import csv
import datetime
import MySQLdb
import simplejson as json
from StringIO import StringIO

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import SimpleStatement
from cassandra.query import ValueSequence

import yaml

total_cnt_cache = {}


def create_connection_cassandra(conf):
    hostname = conf["host"] 
    keyspace = conf["keyspace"]
    nodes = [hostname]
    cluster = Cluster(nodes)
    session = cluster.connect(keyspace)
    return session


def create_connection_mysql(conf):
    db_name = conf["db_name"]
    HOSTNAME = conf["host"]
    USERNAME = conf["username"]
    PASSWD = conf["password"] 
    try:
        conn = MySQLdb.connect(host=HOSTNAME, user=USERNAME, passwd=PASSWD , db=db_name, connect_timeout=28800)
        return conn
    except:
        print("connection to mysql error")
        exit()


def check_req(req, occasion_tag_mapping, role, username):
    filters = req['filters']

    query = {}
    if role != "admin":
        query['assign_annotate'] = "assgin_annotator = '%s'" %(username)

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

    if 'hashtags' in filters.keys() and filters['hashtags'] != [''] and filters['hashtags'] != "" and filters['hashtags'] is not None:
        tmp_str = ','.join(["'" + i + "'" for i in filters['hashtags']])
        query['tag'] = "tag IN (%s)" %(tmp_str)

    if 'occasion' in filters.keys() and filters['occasion'] != "" and filters['occasion'] is not None and filters['occasion'] in occasion_tag_mapping:
        tmp_str = ','.join(["'" + i + "'" for i in occasion_tag_mapping[filters['occasion']]])
        query['tag'] = "tag IN (%s)" %(tmp_str)

    if 'id' in filters.keys() and filters['id'] != "" and filters['id'] is not None:
        query['id'] = "id = '%s'" %(filters['id'])

    if 'annotate_type' in filters.keys() and filters['annotate_type'] is not None and filters['annotate_type'] != "":
        state = filters['annotate_type']
        query['annotate_type'] = "annotate_type = %s" %(state)

    if 'if_annotate_cloth' in filters.keys():
        if filters['if_annotate_cloth'] == True:
            query["if_annotate_cloth"] = "if_annotate_cloth is true" 
        else:
            query["if_annotate_cloth"] = "if_annotate_cloth is false or if_annotate_cloth is NULL" 

    if 'is_face_in_body' in filters.keys() and filters['is_face_in_body'] == True: 
        query['is_face_in_body'] = "is_face_in_body is true"

        per_indexs = {
            'face_per': 'face_percent',       
            'body_per': 'body_percent',       
            'face_body_per': 'face_body_percent',  
            'face_h_per': 'face_h_percent',     
            'body_h_per': 'body_h_percent',     
            'face_body_h_per': 'face_body_h_percent',
            'face_w_per': 'face_w_percent',     
            'body_w_per': 'body_w_percent',     
            'face_body_w_per': 'face_body_w_percent'
        }
    
        for index_q, index_db in per_indexs.items():
            if index_q in filters.keys() and filters[index_q][0] != '' and filters[index_q][0] is not None and filters[index_q][1] != '' and filters[index_q][1] is not None:
                query[index_db] = "%s BETWEEN %s AND %s" %(index_db, filters[index_q][0], filters[index_q][1])

    if 'filter_by_score' in filters.keys() and filters['filter_by_score'] == True: 
        left = filters["binary_score"][0] 
        right = filters["binary_score"][1] 
        if left >= 0 and left <=1 and right >= 0 and right <= 1 and left < right:
            query["binary_score"] = "binary_pred_score BETWEEN %s AND %s" %(left, right)

    if 'must_have_location' in filters.keys() and filters['must_have_location'] == True: 
        query["have_location"] = "parse_cc is not NULL" 

    select_set = []
    if filters['label_yes'] == True:
        select_set.append("1")
    if filters['label_no'] == True:
        select_set.append("0")
    if filters['label_not_sure'] == True:
        select_set.append("2")
    label_filter = ""
    if len(select_set) != 0:
        label_filter = "label_y_n_ns in (%s)" %(",".join(select_set))
    if filters['label_null'] == True:
        if label_filter == "":
            query["label_y_n_ns"] = "label_y_n_ns IS NULL"
        else:
            query["label_y_n_ns"] = label_filter + " OR label_y_n_ns IS NULL"
    else:
        if label_filter != "":
            query["label_y_n_ns"] = label_filter
    #query['label_y_n_ns'] = "label_y_n_ns is NULL OR label_y_n_ns != 0"

    order_by = ""
    if 'if_annotate_cloth' in filters.keys():
        if filters['if_annotate_cloth'] == True:
            order_by = "order by annotate_cloth_datetime desc"

    page_num = int(req['page']['page'])
    if page_num < 1:
        page_num = 1
    batch = int(req['page']['batch'])
    if batch > 1000:
        batch = 1000
    limit = "LIMIT %s, %s" %((page_num - 1) * batch, batch) 

    page_info = {"page": page_num, "batch": batch}
    print "query is:", query
    print "page_num is:", page_num

    return query, order_by, limit, page_info


def get_total_cnt(conn_mysql, q_total_cnt):
    global total_cnt_cache
    print total_cnt_cache
    #if q_total_cnt in total_cnt_cache:
    #    return total_cnt_cache[q_total_cnt] 
    #else:
    mysql_c = conn_mysql.cursor()
    try:
        mysql_c.execute(q_total_cnt)
    except:
        print q_total_cnt
        exit()
    res = mysql_c.fetchall()
    total_cnt = res[0][0]
    #total_cnt_cache[q_total_cnt] = total_cnt
    return total_cnt


def get_task_list_byfile():
    task_list = json.load(open("./data/task_list.json"))
    return task_list


def get_cat_attr_val_list_byfile():
    cat_attr_val_list = json.load(open("./data/clothes_category_attribute_value.json"))
    return cat_attr_val_list


def get_country_list_bydb(conn_mysql):
    country_code_name_mapping = {}
    cnt = 0
    with open('./data/country_name_code_mapping.csv') as csvfile:
        ccreader = csv.reader(csvfile, delimiter=",")
        for row in ccreader:
            if cnt == 0:
                cnt += 1
                continue
            country_code_name_mapping[row[1]] = row[0]

    cc_stat = {}
    mysql_c = conn_mysql.cursor()
    mysql_c.execute("SELECT parse_cc, COUNT(*) AS cnt FROM images GROUP BY parse_cc ORDER BY cnt")
    res = mysql_c.fetchall()
    for i in res:
        cc_stat[i[0]] = i[1]

    result = {}
    for country_code, country_name in country_code_name_mapping.items():
        result[country_code] = {"name": country_name, "num_of_ret": 0}
        if country_code in cc_stat.keys():
            result[country_code]["num_of_ret"] = cc_stat[country_code]

    return {"data": result}


def get_hashtag_list_bydb(conn_mysql):
    hashtag_stat = {}
    mysql_c = conn_mysql.cursor()
    mysql_c.execute("SELECT tag, COUNT(*) AS cnt FROM images GROUP BY tag ORDER BY cnt DESC")
    res = mysql_c.fetchall()
    for i in res:
        hashtag_stat[i[0]] = i[1]

    return {"data": hashtag_stat}


def get_carousel_items_bydb(conn_mysql):
    data_stat = []
    mysql_c = conn_mysql.cursor()
    mysql_c.execute("SELECT COUNT(*) FROM images")
    res = mysql_c.fetchall()
    data_stat.append({"name": "images", "cnt": res[0][0]})

    mysql_c.execute("SELECT COUNT(*) FROM clothes")
    res = mysql_c.fetchall()
    data_stat.append({"name": "clothes", "cnt": res[0][0]})

    return data_stat


def get_a_batch_of_triplets(conn_mysql, req):
    mysql_c = conn_mysql.cursor()
    query = {}

    print(req)

    if "occasion" in req and req["occasion"] and req["occasion"] != "":
        query["occasion"] = "occasion = '%s'" %(req["occasion"])

    if "gender" in req and req["gender"] and req["gender"] != "":
        query["gender"] = "gender = '%s'" %(req["gender"])

    attribute = ""
    value = ""
    if "category" in req and req["category"] and req["category"] != "":
        query["category"] = "category = '%s'" %("__".join(req["category"]))

        if req["attributes"] and len(req["attributes"]) >= 0:
            for attr, val in req["attributes"].items():
                if val and val != "":
                    query[attr] = "%s = '%s'" %(attr, val)
                    attribute = attr
                    value = val
                    # currently, we only consider one attribute for each cloth
                    break

    if len(query) != 0:
        filter_cnd = "WHERE " + " AND ".join(["(" + i + ")" for i in query.values()])
    else:
        filter_cnd = ""

    if attribute != "":
        q_ret = "SELECT occasion, gender, category, %s FROM clothes " %(attribute) + filter_cnd
    else:
        q_ret = "SELECT occasion, gender, category FROM clothes " + filter_cnd
    print("final query SQL is: %s" %(q_ret))

    try: 
        mysql_c.execute(q_ret)
    except:
        print("SQL error: %s" %(q_ret))
        exit()

    res = mysql_c.fetchall()
    res_data = {}
    for i in res:
        occ = i[0]
        gen = i[1]
        cat = i[2]
        if attribute != "":
            triplet = "::".join([occ, gen, cat]) + " " + "::".join([attribute, value])
        else:
            triplet = "::".join([occ, gen, cat])

        if triplet not in res_data:
            res_data[triplet] = 1
        else:
            res_data[triplet] += 1

    new_res = []
    for triplet, cnt in sorted(res_data.items(), key=lambda i: i[1], reverse=True):
        new_res.append([triplet, cnt])

    return new_res

    
def get_a_batch_of_images(conn_mysql, req):
    mysql_c = conn_mysql.cursor()

    #print("\nQuery of images: ")
    #print(json.dumps(req, indent=2))
    query = {}
    
    triplet = req["triplet"]
    x = triplet.split(" ")
    occ_gen_cat, attrs = "", ""
    if len(x) == 2:
        occ_gen_cat, attrs = x
    else:
        occ_gen_cat = x[0]
    occasion, gender, category = occ_gen_cat.split("::")

    if occasion and occasion != "":
        query["occasion"] = "occasion = '%s'" %(occasion)

    if gender and gender != "":
        query["gender"] = "gender = '%s'" %(gender)

    attribute = ""
    value = ""
    if category and category != "":
        query["category"] = "category = '%s'" %(category)

        if attrs != "":
            attribute, value = attrs.split("::")
            query["attribute"] = "%s = '%s'" %(attribute, value)

    # here to record the metadata filter
    filters = req["meta_filters"]
    if 'time' in filters.keys() and filters['time'][0] != '' and filters['time'][0] is not None and filters['time'][1] != '' and filters['time'][1] is not None:
        query['publish_time'] = "publish_time BETWEEN '%s' AND '%s'" %(filters['time'][0], filters['time'][1])

    if 'likes' in filters.keys() and filters['likes'][0] != '' and filters['likes'][0] is not None and filters['likes'][1] != '' and filters['likes'][1] is not None:
        query['likes'] = "likes BETWEEN %s AND %s" %(filters['likes'][0], filters['likes'][1])

    if 'comments' in filters.keys() and filters['comments'][0] != '' and filters['comments'][0] is not None and filters['comments'][1] != '' and filters['comments'][1] is not None:
        query['comments'] = "comments BETWEEN %s AND %s" %(filters['comments'][0], filters['comments'][1])

    if 'bloggers' in filters.keys() and filters['bloggers'] != [''] and filters['bloggers'] is not None:
        tmp_str = ','.join(["'" + i + "'" for i in filters['bloggers']])
        query['blogger'] = "blogger IN (%s)" %(filters['bloggers'])

    if 'country' in filters.keys() and filters['country'] != [''] and filters['country'] != "" and filters['country'] is not None:
        query['country'] = "country = '%s'" %(filters["country"])

    page_num = int(req['config']['page'])
    if page_num < 1:
        page_num = 1
    batch = int(req['config']['batch'])
    if batch > 1000:
        batch = 1000
    limit = "LIMIT %s, %s" %((page_num - 1) * batch, batch) 

    if len(query) != 0:
        filter_cnd = "WHERE " + " AND ".join(["(" + i + ")" for i in query.values()])
    else:
        filter_cnd = ""

    # get total count
    q_total_cnt = "SELECT count(*) FROM clothes " + filter_cnd
    total_cnt = get_total_cnt(conn_mysql, q_total_cnt)
    print("\n\n total cnt is %d" %(total_cnt))

    q_ret = "SELECT * FROM clothes " + filter_cnd + " " + limit
    print "final query is:", q_ret
    mysql_c.execute(q_ret)
    
    res = mysql_c.fetchall()
    res_data = []
    for i in res:
        data = {
            "cloth_id": i[0],
            "img_id": i[1],
            "occasion": i[2],
            "img_src": '/images/' + i[1] + '.jpg',
            "ori_page_src": i[4],
            "text": i[5],
            "blogger": i[6],
            "blogger_page_url": "https://www.instagram.com/" + i[6],
            "likes": i[7],
            "comments": i[8],
            "country": i[9],
            "publish_time": i[10],
            "object_detction": i[11],
            "face_detction": i[12],
            "object_detction_new": i[13],
            "face_detction_new": i[14],
            "clothes": i[15]
        }
        res_data.append(data)

    result = {"pages": {"count": total_cnt}, "data": res_data}

    return result


def get_a_batch_of_data(conn_mysql, order_by, query, limit, page_info):
    mysql_c = conn_mysql.cursor()

    if len(query) != 0:
        filter_cnd = "WHERE " + " AND ".join(["(" + i + ")" for i in query.values()])
    else:
        filter_cnd = ""

    q_total_cnt = "SELECT count(*) FROM images " + filter_cnd
    print "q_total_cnt: ", q_total_cnt
    total_cnt = get_total_cnt(conn_mysql, q_total_cnt)
    page = {'count': total_cnt, "totalPage": total_cnt / page_info['batch'], "limit": page_info['batch'], "page": page_info['page']}
    print "returned page info: ", page

    if order_by != "":
        q_ret = "SELECT * FROM images " + filter_cnd + " " + order_by + " " + limit
    else:
        q_ret = "SELECT * FROM images " + filter_cnd + " " + limit

    print "final query is:", q_ret

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
            "blogger": i[6],
            "blogger_page_url": "https://www.instagram.com/" + i[6],
            "likes": i[7],
            "src_site": "Instagram",
            "object_detction": "",
            "object_detction_new": "",
            "face_detction": "",
            "face_detction_new": "",
            "location_name": "",
            "location_url": i[11],
            "country": "",
            "admin1": "",
            "admin2": "",
            "comments": [12],
            "publish_time": "",
            "location_lat": i[14],
            "location_lon": i[15],
            "width": i[43],
            "height": i[44],
            "clothes": ""
        }
        if i[8] is not None and i[8] != "null":
            data['object_detction'] = i[8]
        if i[9] is not None and i[9] != "null":
            data['face_detction'] = i[9]
        if i[11] != "null":
            data['location_url'] = i[11]
        if i[17] is not None and i[17] != "null":
            data['country'] = i[17]
        if i[18] is not None and i[18] != "null":
            data['admin1'] = i[18]
        if i[19] is not None and i[19] != "null":
            data['admin2'] = i[19]
        if i[32] is not None and i[32] != "null":
            data['object_detction_new'] = i[32]
        if i[33] is not None and i[33] != "null":
            data['face_detction_new'] = i[33]
        if i[35] is not None and i[35] != "null":
            data['clothes'] = i[35]
        try:
            data['text'] = i[4].decode('latin-1')
        except:
            pass
        try:
            data['location_name'] = i[12].decode('latin-1')
        except:
            pass
        try:
            data['publish_time'] = i[13].strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass

        res_data.append(data)

    return {'pages': page, 'data': res_data}


def label_by_req(conn, req):
    res = {"msg": "success"}
    if ("id" not in req) or ("label" not in req) or ("task" not in req):
        res["msg"] = "error: request not complete"
        return res
    elif req["task"] not in ["image_filter"]:
        res["msg"] = "error: task not in permitted list"
        return res
    
    c = conn.cursor()
    if req["task"] == "image_filter":
        if req["label"] == "Yes":
            label = "1"
            c.execute("UPDATE images SET label_y_n_ns = %s WHERE id = %s", [label, req["id"]])
            print "label %s is Yes" %(req["id"])
        if req["label"] == "No":
            label = "0"
            c.execute("UPDATE images SET label_y_n_ns = %s WHERE id = %s", [label, req["id"]])
            print "label %s is No" %(req["id"])
        if req["label"] == "Not Sure":
            label = "2"
            c.execute("UPDATE images SET label_y_n_ns = %s WHERE id = %s", [label, req["id"]])
            print "label %s is Not Sure" %(req["id"])
        conn.commit()
    return res
    

def annotate_clothes_by_req(conn, req, user_name):
    res = {"msg": "success"}
    if ("id" not in req) or ("action" not in req):
        res["msg"] = "error: request not complete"
        return res
    
    c = conn.cursor()
    try:
        c.execute("SELECT clothes FROM images WHERE id = %s", [req["id"]])
    except:
        res["msg"] = "error: query mysql fail"
        return res

    ori_cloth = c.fetchall() 
    if len(ori_cloth) == 0:
        res["msg"] = "error: invalid img id"
        return res
    ori_cloth = json.loads(ori_cloth[0][0])

    if req["action"] == "del_cloth":
        if req['cloth_id'] < 0 or req['cloth_id'] >= len(ori_cloth):
            res["msg"] = "error: invalid cloth id"
            return res
        del ori_cloth[req['cloth_id']]
        try:
            c.execute("UPDATE images SET clothes = %s WHERE id = %s", [json.dumps(ori_cloth), req["id"]])
        except:
            res["msg"] = "error: update mysql fail"
            return res
        conn.commit()

    elif req["action"] == "del_attr":
        if 'cloth_id' not in req or 'index' not in req:
            res["msg"] = "error: request not complete"
            return res
        if req['cloth_id'] < 0 or req['cloth_id'] >= len(ori_cloth):
            res["msg"] = "error: invalid cloth id"
            return res
        if req['index'] < 1 or req['index'] >= len(ori_cloth[req['cloth_id']]['tags']):
            res["msg"] = "error: invalid attr index"
            return res

        del ori_cloth[req['cloth_id']]['tags'][req['index']]
        try:
            c.execute("UPDATE images SET clothes = %s WHERE id = %s", [json.dumps(ori_cloth), req["id"]])
        except:
            res["msg"] = "error: update mysql fail"
            return res
        conn.commit()

    elif req["action"] == "modify_cat_attr":
        if "clothes" not in req:
            res["msg"] = "error: no clothes"
            return res

        for i, new in enumerate(req["clothes"]):
            ori_cat = ""
            for j, ori_tag in enumerate(ori_cloth[i]["tags"]):
                if "category" in ori_tag["tag"]:
                    ori_cat = ori_tag["tag"].split(":")[-1]
                    break
            new_cat = ""
            for j, new_tag in enumerate(new["tags"]):
                if new_tag["cpTag"][0] == "category":
                    new_cat = new_tag["cpTag"][1] 
                    break

            print("ori_cat: %s, new_cat: %s" %(ori_cat, new_cat))
            if ori_cat != new_cat:
                new_tags = []
                for j, new_tag in enumerate(new["tags"]):
                    new_tags.append({"tag": ":".join(new_tag["cpTag"]), "score": 1.0})
                ori_cloth[i]["tags"] = new_tags
            else:
                for j, new_tag in enumerate(new["tags"]):
                    ori_cloth[i]["tags"][j]["tag"] = ":".join(new_tag["cpTag"])
        try:
            c.execute("UPDATE images SET clothes = %s, if_annotate_cloth = %s, annotate_cloth_datetime = %s, annotate_cloth_worker = %s WHERE id = %s", [json.dumps(ori_cloth), 1, datetime.datetime.now(), user_name, req["id"]])
        except:
            res["msg"] = "error: update mysql fail"
            return res
        conn.commit()
        
    else:
        res["msg"] = "error: invalid action"
        return res
 
    return res


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
        return True, res[0][2]
    else:
        return False, ""
