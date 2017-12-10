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

userZip = sys.argv[1]
userLat = float(sys.argv[2])
userLng = float(sys.argv[3])

retDict = {}

obesityValues = list(repo['biel_otis.ObesityData'].find({"cityname": "Boston"}))
propertyValues = list(repo['biel_otis.PropertyValues'].find({"OWNER_MAIL_ZIPCODE": userZip + "_"}))
mapValues = list(repo['biel_otis.BostonZoning'].find())
correlations = list(repo['biel_otis.ObesityPropertyCorrelation'].find())

total_propVal = 0
count = 0
for x in propertyValues:
    total_propVal += float(x['AV_TOTAL'])
    count += 1

retDict['AveragePropVal'] = total_propVal / count

obeseAgg = [('*', 1) for x in obesityValues if Distance((float(x["geolocation"]["latitude"]), float(x["geolocation"]["longitude"])), (userLat, userLng)).miles < 1.0]
total = aggregate(obeseAgg, sum)

retDict['TotalObese'] = total[0][1]

for f in mapValues[0]:
    if (f == '_id'):
        continue
    else:
        if (f == "South Boston" or f == "South Boston Neighborhood"):
            mapValues[0][f]['coordinates'][0][0] = [(x,y) for (y,x) in mapValues[0][f]['coordinates'][0][0]]
        else:
            mapValues[0][f]['coordinates'][0] = [(x,y) for (y,x) in mapValues[0][f]['coordinates'][0]]
    
p = Point(userLat, userLng)
neighborhood = ""
for f in mapValues[0]:
    if (f == "_id"):
        continue
    else:
        poly = shape(mapValues[0][f])
        if (poly.contains(p)):
            neighborhood = f

if (neighborhood != ""):
    retDict['CorrelationCoefficient'] = correlations[0][neighborhood]

print(str(retDict))


