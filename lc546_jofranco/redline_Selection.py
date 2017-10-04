import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class redline_Selection(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.realtime_MBTA']
    writes = ['lc546_jofranco.realtime_MBTAbos']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        
        bos = repo.lc546_jofranco.realtime_MBTA
        
        bos_array = []
        for stop in bos.find():
            for prediction in stop['Predictions']:
                if prediction['Stop'] == 'Charles/MGH' or 'Park Street' or 'Downtown Crossing' or 'South Station' or 'Broadway' or'Andrew':
                    bos_array.append({"Stop":prediction['Stop'], 'info':{"length(seconds)":prediction['Seconds']}})
           # if stop['Predictions'] == 'Charles/MGH' or 'Park Street' or 'Downtown Crossing' or 'South Station' or 'Broadway' or'Andrew':
            #    bos_array.append({"Stop":stop['Stop'], 'info':{"length(seconds)":stop['Seconds']}})
        # Filter: Check if the stops are in Boston, if yes, take in the array

        repo.dropPermanent("realtime_MBTAbos")
        repo.createPermanent("realtime_MBTAbos")
        repo['lc546_jofranco.realtime_MBTAbos'].insert_many(bos_array)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


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
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://developer.mbta.com/lib/')

        this_script = doc.agent('alg:lc546_jofranco#redline_Selection', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'All the realtime_MBTA stops in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_redline_Selection = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_redline_Selection, this_script)
        doc.usage(get_redline_Selection, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        redline_Selection = doc.entity('dat:lc546_jofranco#redline_Selection', {prov.model.PROV_LABEL:'realtime_MBTA stopsp in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(redline_Selection, this_script)
        doc.wasGeneratedBy(redline_Selection, get_redline_Selection, endTime)
        doc.wasDerivedFrom(redline_Selection, resource, get_redline_Selection, get_redline_Selection, get_redline_Selection)

       # repo.record(doc.serialize()) # Record the provenance document.
        #repo.logout()

        return doc

redline_Selection.execute()
doc = redline_Selection.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))