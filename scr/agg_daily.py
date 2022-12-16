import sys
import csv
import numpy
from datetime import timedelta, date, datetime
import time
from pymongo import MongoClient
from tqdm import tqdm
client = MongoClient('mongodb://localhost:27017/')

col_od = client.citi_bike.raw_OD
col_stops = client.citi_bike.stops

rl_od = col_od.find({})
rl_stops = col_stops.find({})
col_stops_insert = client.citi_bike.agg_docks

stops = {}
for ii in rl_stops:
    stops[ii['name']] = ii

speed = 4.5 # 10mph

od = {}

for i in tqdm(rl_od):
    originName = i["ORIGINNAME"]
    destinationName = i["DESTINATIONNAME"]
    originID = i["ORIGINID"]
    originID = i["DESTINATIONID"]
    lat = stops[originName]['lat']
    lon = stops[originName]['lon']
    length = float(i['LENGTH'])
    linktime = length/speed # meter divided by m/s equal to seconds
    try:
        od[originName]
    except:
        od[originName] = {
            "originName": originName,
            "originID": originID,
            "lat": lat,
            "lon": lon,
        }
        for t in [5, 10, 15, 30, 45, 60]:
            od[originName]["access_" + str(t)] = 0
        
    for tt in [5, 10, 15, 30, 45, 60]:
        if linktime < tt * 60:
            od[originName]["access_" + str(tt)] += 1

for aa, ai in od.items():
    col_stops_insert.insert_one(ai)
    
    