
import dml
import prov.model
import datetime
import uuid
import gpxpy.geo
from random import shuffle
from math import sqrt

class newStations(dml.Algorithm):
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
    
    def product(R, S):
        return [(t,u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]


    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.safetyScore']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.newStations']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        safetyScore = repo['jdbrawn_jliang24_slarbi_tpotye.safetyScore'] 

            
        M = [(13,1), (2,12)] #change this
        P = [(1,2),(4,5),(1,3),(10,12),(13,14),(13,9),(11,11)] #change this

        OLD = []
        while OLD != M:
            OLD = M

            MPD = [(m, p, newStations.dist(m,p)) for (m, p) in newStations.product(M, P)]
            PDs = [(p, newStations.dist(m,p)) for (m, p, d) in MPD]
            PD = newStations.aggregate(PDs, min)
            MP = [(m, p) for ((m,p,d), (p2,d2)) in newStations.product(MPD, PD) if p==p2 and d==d2]
            MT = newStations.aggregate(MP, newStations.plus)

            M1 = [(m, 1) for ((m,p,d), (p2,d2)) in newStations.product(MPD, PD) if p==p2 and d==d2]
            MC = newStations.aggregate(M1, sum)

            M = [newStations.scale(t,c) for ((m,t),(m2,c)) in newStations.product(MT, MC) if m == m2]
            print(sorted(M))

        #format it for MongoDB
        location_stations = []
        for entry in M:
            location_stations.append({'New Police Location': entry})

        repo.dropCollection('newStations')
        repo.createCollection('newStations')
        repo['jdbrawn_jliang24_slarbi_tpotye.newStations'].insert_many(location_stations)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_jliang24_slarbi_tpotye/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#newStations', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_safetyScore = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safetyScore', {'prov:label': 'Safety Scores', prov.model.PROV_TYPE: 'ont:DataSet'})


        get_newStations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_newStations, this_script)

        doc.usage(get_newStations, resource_safetyScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        newLocation = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#newStations', {prov.model.PROV_LABEL: 'New Police Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        doc.wasAttributedTo(newLocation, this_script)
        doc.wasGeneratedBy(newLocation, get_newStations, endTime)
        doc.wasDerivedFrom(newLocation, resource_safetyScore, get_newStations, get_newStations, get_newStations)

        repo.logout()

        return doc
