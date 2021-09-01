import json
import sys
import os
def main(path_to_json, output):
    # main logic starts here
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    # file is path to the json file
    data = []
    for json_file in json_files:
    	with open(os.path.join(path_to_json, json_file)) as f:
    		for line in f:
    			data.append(json.loads(line))
    res_dic = {"features": data, "type": "FeatureCollection"}
    with open(output, 'w') as fp:
    	json.dump(res_dic, fp)

if __name__ == '__main__':
    file = sys.argv[1]
    output = sys.argv[2]
    main(file, output)

