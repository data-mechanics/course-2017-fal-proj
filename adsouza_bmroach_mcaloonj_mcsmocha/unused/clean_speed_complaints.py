import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class clean_speed_complaints(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.speed_complaints']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.cleaned_speed_complaints']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        repo.dropCollection('adsouza_bmroach_mcaloonj_mcsmocha.cleaned_speed_complaints')
        repo.createCollection('adsouza_bmroach_mcaloonj_mcsmocha.cleaned_speed_complaints')

        speed_complaints = repo["adsouza_bmroach_mcaloonj_mcsmocha.speed_complaints"].find()

        for complaint in speed_complaints:
            longitude = complaint["X"]
            latitude = complaint["Y"]
            comments = complaint["COMMENTS"]

            new_complaint = {}
            new_complaint["longitude"] = longitude
            new_complaint["latitude"] = latitude
            new_complaint["comments"] = comments

            repo['adsouza_bmroach_mcaloonj_mcsmocha.cleaned_speed_complaints'].insert(new_complaint)



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
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mcj', 'adsouza_bmroach_mcaloonj_mcsmocha')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#clean_speed_complaints', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('mcj:speed_complaints', {'prov:label':'Speed complaints submitted to Vision Zero', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_speed_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_speed_complaints, this_script)

        doc.usage(get_speed_complaints, get_speed_complaints, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        cleaned_speed_complaints = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#cleaned_speed_complaints', {prov.model.PROV_LABEL:'Cleaned Speed Complaints', prov.model.PROV_TYPE:'ont:DataSet'})
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
