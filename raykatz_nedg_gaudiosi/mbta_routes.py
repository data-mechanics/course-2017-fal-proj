import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta_routes(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = []
    writes = ['raykatz_nedg_gaudiosi.mbta_routes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve mbta_routes data from US Census'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        
        with open('auth.json') as data_file:    
            data = json.load(data_file)

        url = "http://realtime.mbta.com/developer/api/v2/routes?api_key=" + data["mbta"] + "&format=json"
        
        
        #Returns the ordered by population numbers of [occupied mbta_routes, vacant mbta_routes,mbta_routes,total mbta_routes,before 1939,total struct age]
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        result = json.loads(response)

        r = []
        for j in result["mode"]:
            for route in j["route"]:
                d = {}
                d["mode_name"] = j["mode_name"]
                d["route_type"] = j["route_type"]
                d["route_id"] = route["route_id"]
                d["route_name"] = route["route_name"]
                r.append(d)

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbta_routes")
        repo.createCollection("mbta_routes")
        repo['raykatz_nedg_gaudiosi.mbta_routes'].insert_many(r)
        repo['raykatz_nedg_gaudiosi.mbta_routes'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.mbta_routes'].metadata())
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
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbta:routes', {'prov:label':'MBTA Routes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mbta_routes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta_routes, this_script)
        
        doc.usage(get_mbta_routes, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?api_key=KEY&format=json'
                  }
                  )
        
        mbta_routes = doc.entity('dat:raykatz_nedg_gaudiosi#mbta_routes', {prov.model.PROV_LABEL:'MBTA Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_routes, this_script)
        doc.wasGeneratedBy(mbta_routes, get_mbta_routes, endTime)
        doc.wasDerivedFrom(mbta_routes, resource, get_mbta_routes, get_mbta_routes, get_mbta_routes)

        repo.logout()
                  
        return doc


'''
mbta_routes.execute()
doc = mbta_routes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
