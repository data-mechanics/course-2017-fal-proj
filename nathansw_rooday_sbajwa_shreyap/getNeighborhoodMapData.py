import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getNeighborhoodMapData(dml.Algorithm):
    contributor = 'nathansw_rooday_sbajwa_shreyap'
    reads = []
    writes = ['nathansw_rooday_sbajwa_shreyap.neighborhoodMap']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')

        print("Fetching neighborhoodMap data...")
        data_url = "http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap/neighborhood_map.json"
        response = requests.get(data_url).json()
        print("neighborhoodMap data fetched!")

        print("Saving neighborhoodMap data...")
        repo.dropCollection("neighborhoodMap")
        repo.createCollection("neighborhoodMap")
        repo['nathansw_rooday_sbajwa_shreyap.neighborhoodMap'].insert(response)
        repo['nathansw_rooday_sbajwa_shreyap.neighborhoodMap'].metadata({'complete':True})
        repo.logout()

        print("Done!")
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
        repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nathansw_rooday_sbajwa_shreyap') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # Since the urls have a lot more information about the resource itself, we are treating everything apart from the actual document suffix as the namespace.
        doc.add_namespace('neighborhoodMap', 'https://data.boston.gov/api/action/datastore_search_sql')

        this_script = doc.agent('alg:nathansw_rooday_sbajwa_shreyap#getNeighborhoodMapData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('neighborhoodMap:?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22', {'prov:label':'neighborhoodMap Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_neighborhoodMap_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighborhoodMap_data, this_script)
        doc.usage(get_neighborhoodMap_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22'
                  }
                  )        
        neighborhoodMap = doc.entity('dat:nathansw_rooday_sbajwa_shreyap#neighborhoodMap', {prov.model.PROV_LABEL:'neighborhoodMap Data', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(neighborhoodMap, this_script)
        doc.wasGeneratedBy(neighborhoodMap, get_neighborhoodMap_data, endTime)
        doc.wasDerivedFrom(neighborhoodMap, resource, get_neighborhoodMap_data, get_neighborhoodMap_data, get_neighborhoodMap_data)

        repo.logout()
        return doc