import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class newbosneighborhoods(dml.Algorithm):
    contributor = 'jtbloom_rfballes_medinad'
    reads = []
    writes = ['jtbloom_rfballes_medinad.neighborhoods']#'medinad.meters'

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')

        url = 'http://datamechanics.io/data/jb_rfb_dm_proj2data/bos_neighborhoods_shapes.json' #'http://bostonopendata-boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)


        neighborhood_list = list(r)
        nid_list = [{'Neighborhood':x["fields"]["neighborho"], 'Geo Shape':x["fields"]["geo_shape"]} for x in neighborhood_list]


        #print(neighborhood_list[0]['fields']['geo_shape']['coordinates'][0][0])

        

        repo.dropCollection("jtbloom_rfballes_medinad.neighborhoods")
        repo.createCollection("jtbloom_rfballes_medinad.neighborhoods")
        repo['jtbloom_rfballes_medinad.neighborhoods'].insert_many(nid_list)
        repo['jtbloom_rfballes_medinad.neighborhoods'].metadata({'complete':True})
        print(repo['jtbloom_rfballes_medinad.neighborhoods'].metadata())

    
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
        pass
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jtbloom_rfballes_medinad', 'jtbloom_rfballes_medinad')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('nei', 'http://datamechanics.io/data/jb_rfb_dm_proj2data/')
        
        this_script = doc.agent('nei:jtbloom_rfballes_medinad#newbosneighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('nei:boston-neighborhoods', {'prov:label':'boston neighborhoods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighbor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighbor, this_script)
        #doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_neighbor, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'/boston-neighborhoods.json'
                  }
                  )
        #doc.usage(get_lost, resource, startTime, None,
        #          {prov.model.PROV_TYPE:'ont:Retrieval',
        #          'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #          }
        #          )

        neighbor = doc.entity('dat:medinad#neighbor', {prov.model.PROV_LABEL:'NEIGHBOR', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighbor, this_script)
        doc.wasGeneratedBy(neighbor, get_neighbor, endTime)
        doc.wasDerivedFrom(neighbor, resource, get_neighbor, get_neighbor, get_neighbor)

        #found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        #doc.wasAttributedTo(found, this_script)
        #doc.wasGeneratedBy(found, get_found, endTime)
        #doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

newbosneighborhoods.execute()
#doc = newbosneighborhoods.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
