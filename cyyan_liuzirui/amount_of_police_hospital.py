from collections import Counter
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class amount_of_police_hospital(dml.Algorithm):
    contributor = 'cyyan_liuzirui'
    reads = ['cyyan_liuzirui.police', 'cyyan_liuzirui.hospital']
    writes = ['cyyan_liuzirui.amount_of_police_hospital']

    @staticmethod
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def project(R, p):
        return [p(t) for t in R]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        # loads police data
        raw_police = repo['cyyan_liuzirui.police'].find()

        # loads hospital data
        raw_hospital = repo['cyyan_liuzirui.hospital'].find()

        # Collection police station zipcode with form {'zipcode':amount}
        p_zipcode = {}
        for i in raw_police:
            if 'location_zip' in i:
                p_zipcode[i['location_zip']] = p_zipcode.get(i['location_zip'], 0) + 1

        # Collection hospital zipcode with form {'zipcode':amount}
        h_zipcode = {}
        for q in raw_hospital:
            if 'ZIPCODE' in q:
                h_zipcode['0' + q['ZIPCODE']] = h_zipcode.get('0' + q['ZIPCODE'], 0) + 1

        # Combine two collections into one
        results = p_zipcode
        for l in h_zipcode:
            if l in results:
                results[l] += h_zipcode[l]
            else:
                results[l] = h_zipcode[l]

        # drop collection amount_of_police_hospital
        repo.dropCollection('amount_of_police_hospital')
        # create collection amount_of_police_hospital
        repo.createCollection('amount_of_police_hospital')
        # insert data in collection amount_of_police_hospital
        repo['cyyan_liuzirui.amount_of_police_hospital'].insert(results)

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
        repo.authenticate('cyyan_liuzirui','cyyan_liuzirui')

        #create document object and define namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/')
        doc.add_namespace('hpa', 'https://data.boston.gov/')
        
        #define entity to represent resources
        this_script = doc.agent('alg:cyyan_liuzirui#amount_of_police_hospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:cyyan_liuzirui#police', {prov.model.PROV_LABEL:'police', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:cyyan_liuzirui#hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        
        ph = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(ph, this_script)

        doc.usage(ph, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(ph, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        p = doc.entity('dat:cyyan_liuzirui#police', {prov.model.PROV_LABEL:'police stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(p, this_script)
        doc.wasGeneratedBy(p, ph, endTime)
        doc.wasDerivedFrom(p, resource1, ph, ph, ph)

        h = doc.entity('dat:cyyan_liuzirui#hospital', {prov.model.PROV_LABEL:'hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(h, this_script)
        doc.wasGeneratedBy(h, ph, endTime)
        doc.wasDerivedFrom(h, resource2, ph, ph, ph)

        repo.logout()
                  
        return doc

# amount_of_police_hospital.execute()
# doc = amount_of_police_hospital.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
