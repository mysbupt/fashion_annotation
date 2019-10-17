#!/usr/bin/env python

def cal_overlap(body_bbox, cloth_bbox):
    x1 = body_bbox[0]
    y1 = body_bbox[1]
    width1 = body_bbox[2] - body_bbox[0]
    height1 = body_bbox[3] - body_bbox[1]
 
    x2 = cloth_bbox[0]
    y2 = cloth_bbox[1]
    width2 = cloth_bbox[2] - cloth_bbox[0]
    height2 = cloth_bbox[3] - cloth_bbox[1]
 
    endx = max(x1+width1, x2+width2)
    startx = min(x1, x2)
    width = width1+width2-(endx-startx)
 
    endy = max(y1+height1,y2+height2)
    starty = min(y1,y2)
    height = height1+height2-(endy-starty)
 
    ratio = 0
    overlap_size = 0
    if width <= 0 or height <= 0:
        pass
    else:
        Area = width * height
        Area2 = width2 * height2
        ratio = Area / Area2
        overlap_size = Area

    return ratio, overlap_size


# each body should have only one face, if one body has more than two faces, it'll be confusing
def align_body_face(persons, faces, image_w, image_h):
    res = {
        "is_face_in_body": False, 
        "face_percent": 0.0, 
        "face_w_percent": 0.0,
        "face_h_percent": 0.0,
        "body_percent": 0.0,
        "body_w_percent": 0.0,
        "body_h_percent": 0.0,
        "face_body_percent": 0.0,
        "face_body_h_percent": 0.0,
        "face_body_w_percent": 0.0,
        "face_body_pair": [],
        "faces": [],
        "bodys": []
    }
    for j, body_bbox in enumerate(persons):
        body_w = body_bbox[2]
        body_h = body_bbox[3]
        half_x_body = body_bbox[2] / 2
        half_y_body = body_bbox[3] / 2
        #the right bottom point x
        body_1 = [body_bbox[0] - half_x_body, body_bbox[1] - half_y_body]
        #the right bottom point y
        body_2 = [body_bbox[0] + half_x_body, body_bbox[1] + half_y_body]

        for i, face_bbox in enumerate(faces):
            face_w = face_bbox[2] - face_bbox[0]
            face_h = face_bbox[3] - face_bbox[1]

            #the left top point x
            face_1 = [face_bbox[0], face_bbox[1]]
            #the left top point y
            face_2 = [face_bbox[2], face_bbox[3]]

            #print face_bbox, body_bbox
            #print ""
            #print face_1, face_2, body_1, body_2
            if face_1[0] >= body_1[0] and face_1[1] >= body_1[1] and face_2[0] <= body_2[0] and face_2[1] <= body_2[1]:
                face_body_pair = {
                    "face": [],
                    "body": [],
                    "face_percent": 0.0,
                    "face_w_percent": 0.0,
                    "face_h_percent": 0.0,
                    "body_percent": 0.0,
                    "body_w_percent": 0.0,
                    "body_h_percent": 0.0,
                    "face_body_percent": 0.0,
                    "face_body_h_percent": 0.0,
                    "face_body_w_percent": 0.0
                }
                
                face_body_pair["face"] = face_bbox
                face_body_pair["body"] = body_bbox
                res["faces"].append(face_bbox)
                res["bodys"].append(body_bbox)

                face_body_pair["face_w_percent"] = face_w / float(image_w)
                face_body_pair["face_h_percent"] = face_h / float(image_h)
                face_body_pair["face_percent"] = face_body_pair["face_w_percent"] * face_body_pair["face_h_percent"] 
                 
                face_body_pair["body_w_percent"] = body_w / float(image_w)
                face_body_pair["body_h_percent"] = body_h / float(image_h)
                face_body_pair["body_percent"] = face_body_pair["body_w_percent"] * face_body_pair["body_h_percent"] 

                face_body_pair["face_body_w_percent"] = face_body_pair["face_w_percent"] / face_body_pair["body_w_percent"] 
                face_body_pair["face_body_h_percent"] = face_body_pair["face_h_percent"] / face_body_pair["body_h_percent"]
                face_body_pair["face_body_percent"] = face_body_pair["face_body_w_percent"] * face_body_pair["face_body_h_percent"] 

                if face_body_pair["body_h_percent"] > res["body_h_percent"]:
                    res["body_percent"] = face_body_pair["body_percent"]
                    res["body_w_percent"] = face_body_pair["body_w_percent"]
                    res["body_h_percent"] = face_body_pair["body_h_percent"]
                    res["face_percent"] = face_body_pair["face_percent"]
                    res["face_w_percent"] = face_body_pair["face_w_percent"]
                    res["face_h_percent"] = face_body_pair["face_h_percent"]
                    res["face_body_percent"] = face_body_pair["face_body_percent"]
                    res["face_body_w_percent"] = face_body_pair["face_body_w_percent"]
                    res["face_body_h_percent"] = face_body_pair["face_body_h_percent"]
                 
                res["face_body_pair"].append(face_body_pair)

    if len(res["face_body_pair"]) != 0:
        res["is_face_in_body"] = True

    return res

#res = align_body_face(faces, persons, data["width"], data["height"])
