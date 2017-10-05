import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transportStops(dml.Algorithm):
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.hubwayFiltered', 'alanbur_jcaluag.mbtaProjected']
    writes = ['alanbur_jcaluag.transportStops']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        hubwayCollection=repo['alanbur_jcaluag.hubwayFiltered'].find()
        mbtaCollection =repo['alanbur_jcaluag.mbtaProjected'].find()
        mbta = [x for x in mbtaCollection]
        hubway = [y for y in hubwayCollection]


        DSet = hubway + mbta

        repo.dropCollection("transportStops")
        repo.createCollection("transportStops")
        repo['alanbur_jcaluag.transportStops'].insert_many(DSet)
        repo['alanbur_jcaluag.transportStops'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.transportStops'].metadata())
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
        
        this_script = doc.agent('alg:alanbur_jcaluag#transportStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:alanbur_jcaluag#hubwayFiltered', {'prov:label':'Filtered Hubway Data', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:alanbur_jcaluag#mbtaProjected', {'prov:label':'Projected MBTA Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_complaints, this_script)
        doc.usage(get_complaints, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.usage(get_complaints, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        roadComplaints = doc.entity('dat:alanbur_jcaluag#transportStops', {prov.model.PROV_LABEL:'MBTA and Hubway Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(roadComplaints, this_script)
        doc.wasGeneratedBy(roadComplaints, get_complaints, endTime)
        doc.wasDerivedFrom(roadComplaints, resource, get_complaints, get_complaints, get_complaints)
        doc.wasDerivedFrom(roadComplaints, resource2, get_complaints, get_complaints, get_complaints)

        repo.logout()
                  
        return doc
#stopsData.execute()