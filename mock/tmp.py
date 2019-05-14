import json

ori = json.load(open("./clothes_category_attribute_value.json"))

res = {}
for i in ori.keys():
    first, second = i.split("__")
    if first not in res:
        res[first] = {second}
    else:
        res[first].add(second)

result = []
for first, seconds in res.items():
    label = first
    x = first.split("_")
    if len(x) > 1:
        label = "".join(x[:-1])
    tmp = {"value": first, "label": label, "children": []}
    for each in sorted(seconds):
        tmp["children"].append({"value": each, "label": each})
    result.append(tmp)

json.dump(result, open("get_categoryTree.json", "w"), indent=4)
