import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict


def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]

def cro(x,y):
    all_cord = []
    for key,value in x.items():
        for c2 in y:
            all_cord.append((key,distance(value,c2)))
    return all_cord

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

def comb(ls):
    result ={}
    for k,v in ls:
        result.setdefault(k,[]).append(v)
    return result

def distance(origin, destination):
    lat1 = origin[0] 
    lon1 = origin[1]
    lat2 = destination[0] 
    lon2 = destination[1]
    radius = 3959 #miles

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d


def counter(ls):
    c = 0
    result =[]
    for k,v in ls.items():
        for i in v:
            if i <=1.5:
                c += 1
            else:
                c == c
        result.append(((k),c))
        c = 0
    return dict(result)

class schoolfinal(dml.Algorithm):
    contributor = 'eileenli_xtq_yidingou'
    reads = ['eileenli_xtq_yidingou.schools', 'eileenli_xtq_yidingou.comfort', 'eileenli_xtq_yidingou.safety', 'eileenli_xtq_yidingou.traffic']
    writes = ['eileenli_xtq_yidingou.schoolfinal', 'eileenli_xtq_yidingou.schoolscore']


    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')

        # loads the collection
        SC = repo['eileenli_xtq_yidingou.schools'].find()

        CM = repo['eileenli_xtq_yidingou.comfort'].find()
        SF = repo['eileenli_xtq_yidingou.safety'].find()
        TR = repo['eileenli_xtq_yidingou.traffic'].find()


        final = []
        score = []
        school_hospital = []

        for i in SC:
            safety = []
            comfort = []
            traffic = []
            entertainment = 0
            restaurant = 0
            hospital = 0
            crime = 0
            crash = 0
            hubway = 0
            signal = 0
            MBTA = 0

            for j in CM:
                temp = []
                for k in j['entertainment']:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        entertainment += 1
                        temp.append(k)
                comfort.append({"entertainment": temp})
                temp = []
                for k in j['restaurants']:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        restaurant += 1
                        temp.append(k)
                comfort.append({"restaurants": temp})
            CM.rewind()

            for j in SF:
                temp = []
                for k in j["hospitals"]:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        hospital += 1
                        temp.append(k)
                safety.append({"hospitals": temp})
                school_hospital.append((i["properties"]["Name"], i["geometry"]["coordinates"], len(temp)))
                temp = []
                for k in j["crimes"]:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        crime += 1
                        temp.append(k)
                safety.append({"crimes": temp})
                temp = []
                for k in j['crash']:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        crash += 1
                        temp.append(k)
                safety.append({"crash": temp})
            SF.rewind()
            
            for j in TR:
                temp = []
                for k in j["crash"]:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        temp.append(k)
                traffic.append({"crash": temp})
                temp = []
                for k in j["hubway"]:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        hubway += 1
                        temp.append(k)
                traffic.append({"hubway": temp})
                temp = []
                for k in j['signals']:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        signal += 1
                        temp.append(k)
                traffic.append({"signals": temp})
                temp = []
                for k in j['MBTA']:
                    if distance(k, i["geometry"]["coordinates"]) <= 2:
                        MBTA += 1
                        temp.append(k)
                traffic.append({"MBTA": temp})
            TR.rewind()


            final.append({
                "school": i["properties"]["Name"],
                "properties": [
                {"coordinates": i["geometry"]["coordinates"]},
                {"safety": safety},
                {"comfort": comfort},
                {"traffic": traffic}]
                })

            score.append({
                "school": i["properties"]["Name"],
                "properties": [
                {"hospital": hospital},
                {"crime": crime},
                {"crash": crash},
                {"restaurant": restaurant},
                {"entertainment": entertainment},
                {"hubway": hubway},
                {"traffic signal": signal},
                {"MBTA": MBTA},
                {"safety": (1000 + hospital * 100 - crime - crash) / 100},
                {"comfort": (restaurant + entertainment) / 100},
                {"traffic": (1500 + MBTA + hubway - signal - crash * 2) / 100}
                ]
                })

        two_school_hospital = select(product(school_hospital, school_hospital), lambda t: t[0][0] != t[1][0])

        for i in two_school_hospital:
            two_school_hospital.remove((i[1], i[0]))

        sum_num = project(two_school_hospital, lambda t: ((t[0][0], t[0][1], t[1][0], t[1][1], t[0][2] + t[1][2])))

        target = ()
        sum_num = select(sum_num, lambda t: t[4] < 10 and distance(t[1], t[3]) < 4)


        if len(sum_num) == 0:
            print("there is no place that can build hospital and benefits 2 or more schools that need hospital")
        else:   
            min_num = 10000
            for i in sum_num:
                if i[4] < min_num:
                    min_num = i[4]
                    target = i
            location = [(target[1][0] + target[3][0]) / 2, (target[1][1] + target[3][1]) / 2]
            print("The best place to build a hospital next is between " + target[0] + " and " + target[2] + " at " + str(location))
            




        repo.dropCollection("schoolfinal")
        repo.createCollection("schoolfinal")

        repo['eileenli_xtq_yidingou.schoolfinal'].insert_many(final)

        repo.dropCollection("schoolscore")
        repo.createCollection("schoolscore")

        repo['eileenli_xtq_yidingou.schoolscore'].insert_many(score)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<fileEnCrime> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#schoolfinal', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_schools = doc.entity('dat:eileenli_xtq_yidingou#schools', {'prov:label': 'schools', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_comfort = doc.entity('dat:eileenli_xtq_yidingou#comfort', {'prov:label': 'comfort', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_safety = doc.entity('dat:eileenli_xtq_yidingou#safety', {'prov:label': 'safety', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_traffic = doc.entity('dat:eileenli_xtq_yidingou#traffic', {'prov:label': 'traffic', prov.model.PROV_TYPE: 'ont:DataSet'})
 
        get = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get, this_script)
        doc.usage(get, resource_schools, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get, resource_comfort, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get, resource_safety, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get, resource_traffic, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})        

        EntSch = doc.entity('dat:eileenli_xtq_yidingou#schoolfinal', {prov.model.PROV_LABEL:'School ranking data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(EntSch, this_script)
        doc.wasGeneratedBy(EntSch, get, endTime)
        doc.wasDerivedFrom(EntSch, resource_schools, get, get, get)
        doc.wasDerivedFrom(EntSch, resource_comfort, get, get, get)
        doc.wasDerivedFrom(EntSch, resource_safety, get, get, get)
        doc.wasDerivedFrom(EntSch, resource_traffic, get, get, get)

        this_script2 = doc.agent('alg:#schoolscore', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        doc.wasAssociatedWith(get, this_script2)
        EntSco = doc.entity('dat:eileenli_xtq_yidingou#schoolscore', {prov.model.PROV_LABEL:'School scores', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(EntSco, this_script2)
        doc.wasGeneratedBy(EntSco, get, endTime)
        doc.wasDerivedFrom(EntSco, resource_schools, get, get, get)
        doc.wasDerivedFrom(EntSco, resource_comfort, get, get, get)
        doc.wasDerivedFrom(EntSco, resource_safety, get, get, get)
        doc.wasDerivedFrom(EntSco, resource_traffic, get, get, get)
        repo.logout()

        return doc

schoolfinal.execute()
doc = schoolfinal.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof