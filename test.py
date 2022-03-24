import sys
sys.path.append("ijson")
import ijson
import json

# read Sydney grid
f = open("data/sydGrid.json","r")
grid  = json.load(f)
for feature in grid["features"]:
    print(feature["properties"])
    print(feature["geometry"]["coordinates"])

#read twitter json
f = open("data/tinyTwitter.json","r")
parses = ijson.parse(f)
language = []
coords = []
for prefix, event, value in parses:
    if prefix == "rows.item.doc.metadata.iso_language_code":
        language.append(value)
    if prefix == "rows.item.doc.coordinates":
        coords.append(value)
