from urllib.request import urlopen
import json
import pymongo
import datetime
import uuid
import time
import ssl
import random
from math import radians, sin, cos, atan2, sqrt
import scipy.stats as ss
from sklearn.cluster import k_means
from geopy.distance import vincenty as Distance
from shapely.geometry import shape, Point, Polygon
import numpy as np
import sys

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

client = pymongo.MongoClient()
repo = client['biel_otis']
repo.authenticate('biel_otis', 'biel_otis')

obesityValues = list(repo['biel_otis.UserObesityData'].find())
mapValues = list(repo['biel_otis.BostonZoning'].find())

obesityLocs = [(x['lat'], x['lng']) for x in obesityValues]

avg_dist = 9999999
num_means = 1
dist_sum = 0
dists = []
means = []

while (avg_dist >= 0.5):
    dist_sum = 0
    means = k_means(obesityLocs, n_clusters=num_means)[0]
    pds = [(p, Distance(m, p).miles) for (m,p) in product(means, obesityLocs)]
    pd = aggregate(pds, min)
    for x in pd:
        dist_sum += x[1]
    avg_dist = dist_sum / len(pd)
    num_means += 1   

correctedMeans = []
means = means.tolist()
inputs = {}
inputs['means'] = means
adjustedMeans = [Point(m) for m in means]
for m in adjustedMeans:
    flag = False
    for f in mapValues[0]:
        if (f == '_id'):
            continue
        else:
            if (f == "South Boston" or f == "South Boston Neighborhood"):
                mapValues[0][f]['coordinates'][0][0] = [(x,y) for (y,x) in mapValues[0][f]['coordinates'][0][0]]
            else:
                mapValues[0][f]['coordinates'][0] = [(x,y) for (y,x) in mapValues[0][f]['coordinates'][0]]
        poly = shape(mapValues[0][f])
        if poly.contains(m):
            flag = True
            break
    correctedMeans.append((str(m), flag))

goodMeans = [x[0] for x in correctedMeans if x[1] == True]
badMeans = [x[0] for x in correctedMeans if x[1] == False]
tempMeans = []
for x in badMeans:
    minDist = 9999999
    minPoint = None
    arr = str(x).replace("(", "").replace(")", "").split(" ")
    coord = (float(arr[1]), float(arr[2]))
    for f in mapValues[0]:
        if (f == '_id'):
            continue
        else:
            if (f == "South Boston" or f == "South Boston Neighborhood"):
                for p in mapValues[0][f]['coordinates'][0][0]:
                    dist = Distance(coord, p).miles
                    if dist < minDist:
                        minDist = dist
                        minPoint = p
            else:
                for p in mapValues[0][f]['coordinates'][0]:
                    dist = Distance(coord, p).miles
                    if dist < minDist:
                        minDist = dist
                        minPoint = p
            
    tempMeans.append(minPoint)
        
finalMeans = []
for x in tempMeans:
    finalMeans.append(x)
for x in goodMeans:
    arr = x.replace("(", "").replace(")", "").split(" ")
    finalMeans.append((float(arr[1]), float(arr[2])))

inputs = {}
inputs['optimalMarketLocation'] = finalMeans

client.biel_otis.biel_otis.UserOptimalMarkets.drop()
client.biel_otis.biel_otis.UserObesityData.drop()
#client.biel_otis.createCollection("biel_otis.UserOptimalMarkets")
repo['biel_otis.UserOptimalMarkets'].insert_many([inputs])
repo.logout()

          

