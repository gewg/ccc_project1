import ijson
import json
from mpi4py import MPI

# check if the coords in in the grid
def coordsInGrid(point, grid):
    lat1 = grid[0][0]
    lat2 = grid[2][0]
    lon1 = grid[2][-1]
    lon2 = grid[0][-1]
    if (lat1 < point[0] and point[0] < lat2):
        if (lon1 < point[1] and point[1] < lon2):
            return True
    return False

# read Sydney grid
f = open("sydGrid-2.json","r")
grid  = json.load(f)
gridDict = {}
for feature in grid["features"]:
    gridDict[feature["properties"]["id"]] = feature["geometry"]["coordinates"][0]
gridDict = dict(sorted(gridDict.items()))

# read twitter json
f = open("smallTwitter.json","r")
datas = ijson.items(f,'rows.item')
datas = list(datas)
filterd_data = [data for data in datas if data["doc"]["coordinates"]!=None]
for data in filterd_data:
    data["doc"]["coordinates"]["coordinates"] = [float(point) for point in data["doc"]["coordinates"]["coordinates"]]

# filter out the data that are not in the grid
inRange_data = []
for data in filterd_data:
    for key,value in gridDict.items():
        if coordsInGrid(data["doc"]["coordinates"]["coordinates"],value):
            inRange_data.append(data)
            break

#TODO: 1. make the inRange_data to dict, and count by the grid id
#      2. count language in the each region
#      3. Find the top ten language overall
#      4. Mutilthread and run on the slurm