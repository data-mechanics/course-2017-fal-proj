import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class income_zipcode(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.getemployeeearnings']
    writes = ['lc546_jofranco.income_zipcode']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        income = repo['lc546_jofranco.income']
        
        zipcode = []
        for zips in income.find():
            zipcode.append((zips['postal'], 1))

        #collect all the zipcodes


        keys = {r[0] for r in zipcode}
        zipcode = [(key, medium([v for (k,v) in zipcode if k == key])) for key in keys]

        insert = []
        for x in zipcode:
            insert.append({"zip":x[0],'info': {"numberOfSchoolAndHospital":x[1]}})
            

        repo.dropPermanent("income_zipcode")
        repo.createPermanent("income_zipcode")
        repo['lc546_jofranco.income_zipcode'].insert_many(insert)



        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.Provzips(), startTime = None, endTime = None):
        '''
        Create the provenance zips describing everything happening
        in this script. Each run of the script will generate a new
        zips describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:lc546_jofranco#income_zipcode', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'School and Hospital Number in each zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income_zipcode = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income_zipcode, this_script)
        doc.usage(get_income_zipcode, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        income_zipcode = doc.entity('dat:lc546_jofranco#income_zipcode', {prov.model.PROV_LABEL:'School and Hospital number in each zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income_zipcode, this_script)
        doc.wasGeneratedBy(income_zipcode, get_income_zipcode, endTime)
        doc.wasDerivedFrom(income_zipcode, resource, get_income_zipcode, get_income_zipcode, get_income_zipcode)

        repo.record(doc.serialize()) # Record the provenance zips.
        repo.logout()

        return doc

income_zipcode.execute()
doc = income_zipcode.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))