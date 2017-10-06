import urllib.request
import json
import dml, prov.model
import datetime, uuid
import geojson

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

Aggregate Hubway trips by Boston neighborgood

Development notes:


"""

class transform_open_space(dml.Algorithm):
    contributor = 'bmroach'
    reads = ['bmroach.open_space']
    writes = []

    @staticmethod
    def execute(trial = False, log=False):
        startTime = datetime.datetime.now()
        

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        #Read in  data
        ownershipSums = {}
        totalSum = 0
        open_space = repo.bmroach.open_space.find()
        for entry in open_space:
            if entry['properties']['ACRES']  == '' \
            or entry['properties']['OWNERSHIP'] == '' or\
            entry['properties']['ACRES']  == None \
            or entry['properties']['OWNERSHIP'] == None:
                continue

            try:
                a = int(entry['properties']['ACRES'])
            except:
                print(entry['properties']['ACRES'])
                return
            o = entry['properties']['OWNERSHIP']
            
            

            if o in ownershipSums:
                ownershipSums[o] += a
            else:
                ownershipSums[o] = a
            totalSum += a

        
        #change to percent
        for key, val in ownershipSums.items():
            ownershipSums[key] /= totalSum


        with open("./custom_output_datasets/open_space_ownership.json", 'w') as outfile:
            json.dump(ownershipSums, outfile)

        
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

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.        

        this_script = doc.agent('alg:bmroach#transform_open_space', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('dat:bmroach#open_space', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        transform = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(transform, this_script)
        
        doc.usage(transform,resource, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':''  
                            }
                            )
        
        

        open_space_owners = doc.entity('dat:bmroach#open_space_owners', {prov.model.PROV_LABEL:'open_space_owners', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(open_space_owners, this_script)
        doc.wasGeneratedBy(open_space_owners, transform, endTime)
        
        doc.wasDerivedFrom(open_space_owners, resource, transform, transform, transform)
      
        repo.logout()                  
        return doc




# transform_open_space.execute(log=True)

# doc = transform_open_space.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
