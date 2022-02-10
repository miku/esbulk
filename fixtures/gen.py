import json
for i in range(10000):
    print(json.dumps({"v": "{}".format(i)}))
