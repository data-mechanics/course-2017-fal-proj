import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class filterHubway(dml.Algorithm):
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.hubwayProjected']
    writes = ['alanbur_jcaluag.hubwayFiltered']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        collection=repo['alanbur_jcaluag.hubwayProjected'].find()

        #Boston Coordinate range
        northLat = 42.392459
        southLat = 42.243896
        eastLong = -71.03568
        westLong = -71.187272

        DSet = [x for x in collection]


        DSet = [x for x in DSet if ((southLat < x['Latitude'] < northLat)) and ((eastLong > x['Longitude'] > westLong))]


        repo.dropCollection("hubwayFiltered")
        repo.createCollection("hubwayFiltered")
        repo['alanbur_jcaluag.hubwayFiltered'].insert_many(DSet)
        repo['alanbur_jcaluag.hubwayFiltered'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.hubwayFiltered'].metadata())
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
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:alanbur_jcaluag#filterHubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_filter = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_filter, this_script)
        doc.usage(get_filter, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        projected = doc.entity('dat:alanbur_jcaluag#filterHubway', {prov.model.PROV_LABEL:'Filtered Hubway Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(projected, this_script)
        doc.wasGeneratedBy(projected, get_filter, endTime)
        doc.wasDerivedFrom(projected, resource, get_filter, get_filter, get_filter)

        repo.logout()
                  
        return doc
    
#filterData.execute()