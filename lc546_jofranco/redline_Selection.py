import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class bos_redline(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.redline']
    writes = ['lc546_jofranco.redlinebos']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        
        bos = repo['lc546_jofranco.redline']
        
        bos_array = []
        for stop in bos.find():
            if stop['Stop'] == 'Charles/MGH'|'Park Street'|'Downtown Crossing'|'South Station'|'Broadway'|'Andrew':
                bos_array.append({"Stop":stop['Stop'], 'info':{"length(seconds)":stop['Seconds']}})
        # Filter: Check if the stops are in Boston, if yes, take in the array

        repo.dropPermanent("redlinebos")
        repo.createPermanent("redlinebos")
        repo['lc546_jofranco.redlinebos'].insert_many(bos_array)


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

        this_script = doc.agent('alg:lc546_jofranco#bos_redline', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'All the redline stops in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bos_redline = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bos_redline, this_script)
        doc.usage(get_bos_redline, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        bos_redline = doc.entity('dat:lc546_jofranco#bos_redline', {prov.model.PROV_LABEL:'Redline stopsp in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bos_redline, this_script)
        doc.wasGeneratedBy(bos_redline, get_bos_redline, endTime)
        doc.wasDerivedFrom(bos_redline, resource, get_bos_redline, get_bos_redline, get_bos_redline)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

bos_redline.execute()
doc = bos_redline.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))