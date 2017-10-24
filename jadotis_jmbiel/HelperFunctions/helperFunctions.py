
"""
Helper Functions courtesy of Andrei Lapets - CS591 BU
"""
from math import sin, cos, sqrt, atan2, radians
import math

# approximate radius of earth in km

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


def calculateDist(d1, d2):
    R = 6373.0
    d1 = d1.replace("(", "").replace(")", "")
    d1 = d1.split(",")
    d1 = (float(d1[0]), float(d1[1]))

    d2 = d2.replace("(", "").replace(")", "")
    d2 = d2.split(",")
    d2 = (float(d2[0]), float(d2[1]))

    lat1 = radians(d1[0])
    lon1 = radians(d1[1])
    lat2 = radians(d2[0])
    lon2 = radians(d2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance <= 1


def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def compTuples(t1, t2):
    if(t1 == []):
        return 100000000000000
    comp = [abs(x[0] - y[0]) + abs(x[1] - y[1]) for x in t1 for y in t2]
    return sum(comp)

#print(compTuples([(50, 50), (60,60), (70,70), (80,80), (90,90), (100,100), (110,110), (120,120), (130,130), (140,140), (150,150)], [(51, 51), (61,61), (71,71), (81,81), (91,91), (101,101), (111,111), (121,121), (131,131), (141,141), (151,151)]))