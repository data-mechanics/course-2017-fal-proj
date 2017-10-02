import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests


class transformation1(dml.Algorithm):
    def union(R, S):
        return R + S
    def intersect(R, S):
        return [t for t in R if t in S]
    def product(R, S):
        return [(t,u) for t in R for u in S]
    def select(R, s):
        return [t for t in R if s(t)]
    def aggregate(R):
        keys = {r[0] for r in R}
        return [(key, [v for (k,v) in R if k == key]) for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    def removeDuplicates(seq):
        #helper function from previous semester
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x)) and x != " "]


    contributor = 'jdbrawn_slarbi'
    reads = ['jdbrawn_slarbi.entertain',
              'jdbrawn_slarbi.food']
    writes = ['jdbrawn_slarbi.socialAnalysis']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        entertainmentLoc = repo['jdbrawn_slarbi.entertain']
        foodLoc = repo['jdbrawn_slarbi.food']

        #begin transformation
        
        foodZips = []
        entertainmentZips = []
        finalList = []
        for entry in foodLoc.find():
            if 'zip' in entry:
                foodZips.append((entry['zip'], entry['businessname']))

        for entry in entertainmentLoc.find():
            if 'zip' in entry:
                entertainmentZips.append((entry['zip'], entry['businessname']))
#            entertainmentZips.append({'zipcode': entry['zip'], "name": entry['businessname']})

        both = transformation1.union(foodZips, entertainmentZips)
        both = transformation1.removeDuplicates(both)
        combo = transformation1.aggregate(both)
        for entry in combo:
            finalList.append({'zipcode':entry[0], 'numSocialBusinesses':len(entry[1])})
       # print(finalList)
            



        print('DONE!')
        repo.dropCollection('socialAnalysis')
        repo.createCollection('socialAnalysis')
        repo['jdbrawn_slarbi.socialAnalysis'].insert_many(finalList)

        
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
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jdbrawn_slarbi') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jdbrawn_slarbi') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
      


        this_script = doc.agent('alg:jdbrawn_slarbi#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource1 = doc.entity('bdp:cz6t-w69j', {'prov:label':'Entertainment Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_entertainment_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_entertainment_data, this_script)
        doc.usage(get_entertainment_data, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                
                }
                )
        
        resource2 = doc.entity('bdp:fdxy-gydq', {'prov:label':'Food License Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_food_license = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_food_license, this_script)
        doc.usage(get_food_license, resource2, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                
                }
                )



        entertainment_data = doc.entity('dat:jdbrawn_slarbi#entertain', {prov.model.PROV_LABEL:'Entertainment Data', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(entertainment_data, this_script)
        doc.wasGeneratedBy(entertainment_data, get_entertainment_data, endTime)
        doc.wasDerivedFrom(entertainment_data, resource1, get_entertainment_data, get_entertainment_data, get_entertainment_data)

        food_license = doc.entity('dat:jdbrawn_slarbi#food', {prov.model.PROV_LABEL:'Food License Data', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(food_license, this_script)
        doc.wasGeneratedBy(food_license, get_food_license, endTime)
        doc.wasDerivedFrom(food_license, resource2, get_food_license, get_food_license, get_food_license)



        repo.logout()

        return doc

# transformation1.execute()
# doc = transformation1.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


