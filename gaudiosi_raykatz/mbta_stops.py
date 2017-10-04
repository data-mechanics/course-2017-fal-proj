import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta_stops(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = ['gaudiosi_katz.mbta_routes']
    writes = ['gaudiosi_katz.mbta_stops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve mbta_stops data from realtime.mbta.com'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        
        
        
        with open('auth.json') as data_file:    
                data = json.load(data_file)
        
        routes = list(repo.gaudiosi_raykatz.mbta_routes.find({}))
        
        r = []
        for route in routes:
            url = "http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=" + data["mbta"] + "&route=" + route["route_id"] +  "&format=json"
            response = urllib.request.urlopen(url).read().decode("utf-8")        
            stops = json.loads(response)
            for direction in stops["direction"]:
                for stop in direction["stop"]:
                    s = {}                   
                    s["mode_name"] = route["mode_name"]
                    s["route_id"] = route["route_id"]
                    s["route_name"] = route["route_name"]
                    s["direction"] = direction["direction_name"]
                    s["stop_order"] = stop["stop_order"]
                    s["stop_id"] = stop["stop_id"]
                    s["stop_name"] = stop["stop_name"]
                    s["parent_station"] = stop["parent_station"]
                    s["parent_station_name"] = stop["parent_station_name"]
                    s["stop_lat"] = stop["stop_lat"]
                    s["stop_lon"] = stop["stop_lon"]
                    r.append(s)
        
        s = json.dumps(r, sort_keys=True, indent=2)        
        repo.dropCollection("mbta_stops")
        repo.createCollection("mbta_stops")
        repo['gaudiosi_raykatz.mbta_stops'].insert_many(r)
        repo['gaudiosi_raykatz.mbta_stops'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.mbta_stops'].metadata())
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
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gaudiosi_raykatz#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mbta_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta_stops, this_script)
        
        doc.usage(get_mbta_stops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=MBTA_Routes&$select=mode_name,route_type,route_id,route_name'
                  }
                  )
        
        mbta_stops = doc.entity('dat:gaudiosi_raykatz#mbta_stops', {prov.model.PROV_LABEL:'MBTA Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_stops, this_script)
        doc.wasGeneratedBy(mbta_stops, get_mbta_stops, endTime)
        doc.wasDerivedFrom(mbta_stops, resource, get_mbta_stops, get_mbta_stops, get_mbta_stops)

        repo.logout()
                  
        return doc

## eof
