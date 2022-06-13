import requests
import random
from pymongo import MongoClient
from multiprocessing.pool import ThreadPool
import time
import csv
import os
import re, socket

bikeURL = "https://gbfs.cogobikeshare.com/gbfs/en/station_status.json"

client = MongoClient('mongodb://localhost:27017/')

db_bike = client.cogo_gbfs

session = requests.Session()
interval = 60

def requestFeed():
    ts = int(time.time())
    roundts = int(time.time()/5)*5
    col = db_bike["cogo_gbfs_raw"]
    try:
        r = session.get(url=bikeURL)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
        return False
                  
    data = r.json()
    # print(data)
    station_data = data['data']['stations']
    updatets = data['last_updated']
    for i in station_data:
        line = i
        line['ts'] = ts
        line['updatets'] = updatets
        line['roundts'] = roundts
        col.insert_one(line)


if __name__ == "__main__":
    requestFeed()
    
    