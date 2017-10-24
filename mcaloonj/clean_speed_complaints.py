import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class clean_speed_complaints(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = ['mcaloonj.speed_complaints']
    writes = ['mcaloonj.cleaned_speed_complaints']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        repo.dropCollection('mcaloonj.cleaned_speed_complaints')
        repo.createCollection('mcaloonj.cleaned_speed_complaints')

        speed_complaints = repo["mcaloonj.speed_complaints"].find()

        for complaint in speed_complaints:
            longitude = complaint["X"]
            latitude = complaint["Y"]
            comments = complaint["COMMENTS"]

            new_complaint = {}
            new_complaint["longitude"] = longitude
            new_complaint["latitude"] = latitude
            new_complaint["comments"] = comments

            repo['mcaloonj.cleaned_speed_complaints'].insert(new_complaint)



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

        this_script = doc.agent('alg:mcaloonj#clean_speed_complaints', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('mcj:speed_complaints', {'prov:label':'Speed complaints submitted to Vision Zero', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_speed_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_speed_complaints, this_script)

        doc.usage(get_speed_complaints, get_speed_complaints, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        cleaned_speed_complaints = doc.entity('dat:mcaloonj#cleaned_speed_complaints', {prov.model.PROV_LABEL:'Cleaned Speed Complaints', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cleaned_speed_complaints, this_script)
        doc.wasGeneratedBy(cleaned_speed_complaints, get_speed_complaints, endTime)
        doc.wasDerivedFrom(cleaned_speed_complaints, resource, get_speed_complaints, get_speed_complaints, get_speed_complaints)

        repo.logout()

        return doc
'''
clean_speed_complaints.execute()
doc = clean_speed_complaints.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
##eof
