import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from builtins import staticmethod

class avgTrafficDelay(dml.Algorithm):
    contributor = 'peterg04_yfchen'
    reads = ['peterg04_yfchen.traffic']
    writes = ['peterg04_yfchen.avgTrafficDelay']
        
    @staticmethod
    def execute(trial = False):
        # helper functions from lecture 591 by Lapets
        def select(R, s):
            return [t for t in R if s(t)]
        
        def project(R, p):
            return [p(t) for t in R]
        
        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k,v) in R if k == key])) for key in keys]
        
        startTime = datetime.datetime.now()
        
        # Set up the db connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('peterg04_yfchen', 'peterg04_yfchen')
        
#         response = urllib.request.urlopen(url).read().decode("utf-8")
#         r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("avgTrafficDelay")
        repo.createCollection("avgTrafficDelay")
        
        # set trafficData to a variable for manipulation
        trafficData = repo[avgTrafficDelay.reads[0]].find()  # a list of dictionaries
        
        # We will be doing a selection -> projection -> aggregation -> projection = new dataset
        # need to drop the first dictionary entry of the list ... it doesn't go with the schema of the rest of data set for some reason and causes error
        selectedData = select(trafficData, lambda entry: "city" in entry)
#         
         # Selection : will be based on the city so that I obtain all of the Boston data only.
        selectedData = select(selectedData, lambda entry: entry["city"].startswith('Boston'))
        
        # Projection : I want to project the data so that it becomes {date : delay}
        projectedData = project(selectedData, lambda entry: (entry["inject_date"], int(entry["delay"])))
        
        # Aggregation will allow us to add up all the delay times of the traffic delays on those certain days/times
        aggregatedData = aggregate(projectedData, sum)
        
        # Must do projection one last time to get the mean of delay time for each date; convert back to a dictionary
        finalData = project(aggregatedData, lambda entry: dict([(entry[0], entry[1]/len(projectedData))]))
        
        repo['peterg04_yfchen.avgTrafficDelay'].insert(finalData, check_keys = False)
        repo['peterg04_yfchen.avgTrafficDelay'].metadata({'complete':True})
        print(repo['peterg04_yfchen.avgTrafficDelay'].metadata())
        
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
        repo.authenticate('peterg04_yfchen', 'peterg04_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:peterg04_yfchen#avgTrafficDelay', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_avgTrafficDelay = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_avgTrafficDelay, this_script)
        doc.usage(get_avgTrafficDelay, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        avgTrafficDelay= doc.entity('dat:peterg04_yfchen#avgTrafficDelay', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(avgTrafficDelay, this_script)
        doc.wasGeneratedBy(avgTrafficDelay, get_avgTrafficDelay, endTime)
        doc.wasDerivedFrom(avgTrafficDelay, resource, get_avgTrafficDelay, get_avgTrafficDelay, get_avgTrafficDelay)

        repo.logout()
                  
        return doc
        
        
        
    
    