from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import time
import ssl
from biel_otis.HelperFunctions.helperFunctions import *

class setObesityMarkets(dml.Algorithm):
    contributor = 'biel_otis'
    reads = ['biel_otis.ObesityData']
    writes = ['biel_otis.OptimalMarketLoc']

    @staticmethod
    def execute(trial = False):        
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')

        obesityValues = list(repo['biel_otis.ObesityData'].find())

        #selection of the geolocation of overweight individuals in Boston
        obeseLocations = [x['geolocation'] for x in obesityValues if x['measureid'] == 'OBESITY' and x['cityname'] == 'Boston']
        latAndLong = [(float(x['latitude']),float(x['longitude'])) for x in obeseLocations]
        means = [(520.23, 50.32), (642.2,6230.1), (702.32,72.2), (832.23,8.20), (902.2,902.2), (111.1,92.2), (-123.1,23.1), (-32.1,-74.2), (30.32,-10.2), (14.2,343.2), (-2.2,15.20)]
        #print(latAndLong)

        old = []
        while (old != means):
            old = means
            mpd = [(m, p, dist(m, p)) for (m,p) in product(means, latAndLong)]
            pds = [(p, dist(m,p)) for (m, p, d) in mpd]
            pd = aggregate(pds, min)
            mp = [(m, p) for ((m,p,d), (p2,d2)) in product(mpd, pd) if p==p2 and d==d2]
            mt = aggregate(mp, plus)
            m1 = [(m, 1) for ((m,p,d), (p2, d2)) in product(mpd, pd) if p==p2 and d==d2]
            mc = aggregate(m1, sum)

            means = [scale(t, c) for ((m,t), (m2,c)) in product(mt, mc) if m == m2]
        inputs = [{'latitude': x[0], 'longitude': x[1]} for x in means]

        repo.dropCollection("OptimalMarketLoc")
        repo.createCollection("OptimalMarketLoc")
        repo['biel_otis.OptimalMarketLoc'].insert_many(inputs)
        repo['biel_otis.OptimalMarketLoc'].metadata({'complete':True})
        print(repo['biel_otis.OptimalMarketLoc'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client['biel_otis']
        repo.authenticate('biel_otis', 'biel_otis')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:biel_otis#setObesityMarkets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        obesity = doc.entity('dat:biel_otis#obesity', {prov.model.PROV_LABEL:'Obesity Data from City of Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        obesityMarkets = doc.entity('dat:biel_otis#obesity_market_locations', {prov.model.PROV_LABEL:'Dataset containing the optimal locations for healty food markets', prov.model.PROV_TYPE:'ont:DataSet'})


        get_obesityMarkets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_obesityMarkets, this_script)
        
        doc.usage(get_obesityMarkets, obesity, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Transformation'})

        doc.wasAttributedTo(obesityMarkets, this_script)
        doc.wasGeneratedBy(obesityMarkets, obesityMarkets, endTime)
        doc.wasDerivedFrom(obesity, get_obesityMarkets, get_obesityMarkets, get_obesityMarkets, get_obesityMarkets, get_obesityMarkets)
        repo.logout()
        
        return doc

getObesityMarkets.execute()
doc = getObesityMarkets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
