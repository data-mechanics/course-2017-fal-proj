import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import numpy



class income_aggregate(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.employee_earnings']
    writes = ['lc546_jofranco.income_aggregate']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        income = repo.lc546_jofranco.employee_earnings
        
        zipcode = []
        for zips in income.find():
            zipcode.append((zips['postal'], float(zips['total_earnings'])))
        #print(zipcode)
        #Take the zipcodes and income out
    
    	
        #aggregate it. Find the medium of income of each zipcode
        agg_income = income_aggregate.aggregate(zipcode, sum)
      #  print("INCOME MEAN",dict(agg_income))

        repo.dropCollection("income_aggregate")
        repo.createCollection("income_aggregate")
        repo['lc546_jofranco.income_aggregate'].insert_many([dict(agg_income)])
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance zips describing everything happening
        in this script. Each run of the script will generate a new
        zips describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:lc546_jofranco#income_aggregate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'The medium income of the population in each zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income_aggregate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income_aggregate, this_script)
        doc.usage(get_income_aggregate, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        income_aggregate = doc.entity('dat:lc546_jofranco#income_aggregate', {prov.model.PROV_LABEL:'The medium income of the population in each zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income_aggregate, this_script)
        doc.wasGeneratedBy(income_aggregate, get_income_aggregate, endTime)
        doc.wasDerivedFrom(income_aggregate, resource, get_income_aggregate, get_income_aggregate, get_income_aggregate)

      #  repo.record(doc.serialize()) # Record the provenance zips.
       # repo.logout()

        return doc

income_aggregate.execute()
doc = income_aggregate.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
