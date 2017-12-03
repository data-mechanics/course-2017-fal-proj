import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class getMBTAPerformanceData(dml.Algorithm):
    contributor = 'nathansw_rooday_sbajwa_shreyap'
    reads = []
    writes = ['nathansw_rooday_sbajwa_shreyap.MBTAPerformance', 'nathansw_sbajwa.householdincome', 'nathansw_sbajwa.povertyrates', 'nathansw_sbajwa.commuting']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nathansw_rooday_sbajwa_shreyap', 'nathansw_rooday_sbajwa_shreyap')

        print("Fetching MBTAPerformance data...")
        data_url = "http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap/MBTAPerformance.json"
        response = requests.get(data_url).json()
        print("MBTAPerformance fetched!")

        count = 0
        obj1 = {}
        obj2 = {}
        obj3 = {}
        for key in response.keys():
          if count % 3 == 0:
            obj1[key] = response[key]
          elif count % 3 == 1:
            obj2[key] = response[key]
          elif count % 3 == 2:
            obj3[key] = response[key]
          count += 1

        final = [obj1, obj2, obj3]

        print("Saving MBTAPerformance data...")
        repo.dropCollection("MBTAPerformance")
        repo.createCollection("MBTAPerformance")
        if trial:
          repo['nathansw_rooday_sbajwa_shreyap.MBTAPerformance'].insert_one(final[0])
        else:
          repo['nathansw_rooday_sbajwa_shreyap.MBTAPerformance'].insert_many(final)
        repo['nathansw_rooday_sbajwa_shreyap.MBTAPerformance'].metadata({'complete':True})
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

        #Since the urls have a lot more information about the resource itself, we are treating everything apart from the actual document suffix as the namespace.
        doc.add_namespace('MBTAPerformance', 'https://data.boston.gov/api/action/datastore_search_sql')

        this_script = doc.agent('alg:#getMBTAPerformanceData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('MBTAPerformance:?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22', {'prov:label':'MBTAPerformance Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_MBTAPerformance_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_MBTAPerformance_data, this_script)
        doc.usage(get_MBTAPerformance_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22'
                  }
                  )        
        MBTAPerformance = doc.entity('dat:#MBTAPerformance', {prov.model.PROV_LABEL:'MBTAPerformance Data', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(MBTAPerformance, this_script)
        doc.wasGeneratedBy(MBTAPerformance, get_MBTAPerformance_data, endTime)
        doc.wasDerivedFrom(MBTAPerformance, resource, get_MBTAPerformance_data, get_MBTAPerformance_data, get_MBTAPerformance_data)

        repo.logout()
        return doc