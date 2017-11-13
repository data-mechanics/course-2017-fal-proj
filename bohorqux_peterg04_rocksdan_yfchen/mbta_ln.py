import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta_ln(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.MBTA']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.mbta_late_nights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')


        reports = repo['bohorqux_peterg04_rocksdan_yfchen.MBTA']

        wdk = {0:"Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday", 5:"Saturday", 6:"Sunday"}
        ln = {"Monday":0, "Tuesday":0, "Wednesday":0, "Thursday":0, "Friday":0, "Saturday":0, "Sunday":0}
        
        
        for r in reports.find():
            try:
                if r['latenightroute'] == "1":
                    x = r['scheduledate'].split("/")
                    month = int(x[0])
                    day = int(x[1])
                    year = int(x[2])
                    weekday = datetime.date(year, month, day).weekday()
                    
                    ln[wdk[weekday]] += 1
                    
            except ValueError:
                pass
        
        lst = [ln]
        repo.dropCollection("mbta_late_nights")
        repo.createCollection("mbta_late_nights")
        repo['bohorqux_rocksdan.mbta_late_nights'].insert_many(lst)
        repo['bohorqux_rocksdan.mbta_late_nights'].metadata({'complete':True})
        print(repo['bohorqux_rocksdan.mbta_late_nights'].metadata())

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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bohorqux_rocksdan#mbta_ln', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mbta_late_nights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta_late_nights, this_script)
        doc.usage(get_mbta_late_nights, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?line=Bus&$select=line,trxdow'
                  }
                  )
        mbta_late_nights = doc.entity('dat:bohorqux_rocksdan#mbta_late_nights', {prov.model.PROV_LABEL:'MBTA Late Nights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_late_nights, this_script)
        doc.wasGeneratedBy(mbta_late_nights, get_mbta_late_nights, endTime)
        doc.wasDerivedFrom(mbta_late_nights, resource, get_mbta_late_nights, get_mbta_late_nights, get_mbta_late_nights)

        repo.logout()
                  
        return doc

mbta_ln.execute()
doc = mbta_ln.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
