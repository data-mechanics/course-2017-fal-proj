import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class clean_accidents(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = ['mcaloonj.accidents']
    writes = ['mcaloonj.cleaned_accidents']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        repo.dropCollection('mcaloonj.cleaned_accidents')
        repo.createCollection('mcaloonj.cleaned_accidents')

        accidents = repo["mcaloonj.accidents"].find()

        for a in accidents:
            try:
                longitude = float(a["Long"])
                latitude = float(a["Lat"])

                new_a = {}
                new_a["longitude"] = longitude
                new_a["latitude"] = latitude

                repo['mcaloonj.cleaned_accidents'].insert(new_a)
            except:
                None

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
        repo.authenticate('mcaloonj', 'mcaloonj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mcj', 'mcaloonj')

        this_script = doc.agent('alg:mcaloonj#clean_accidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('mcj:accidents', {'prov:label':'Accidents', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_accidents, this_script)

        doc.usage(get_accidents, get_accidents, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        clean_accidents = doc.entity('dat:mcaloonj#cleaned_accidents', {prov.model.PROV_LABEL:'Cleaned Accidents', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clean_accidents, this_script)
        doc.wasGeneratedBy(clean_accidents, get_accidents, endTime)
        doc.wasDerivedFrom(clean_accidents, resource, get_accidents, get_accidents, get_accidents)

        repo.logout()

        return doc
'''
clean_accidents.execute()
doc = clean_accidents.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

##eof
