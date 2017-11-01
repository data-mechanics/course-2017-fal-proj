import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class bus_stop(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = []
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.bus_stop']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui','yjunchoi_yzhang71_cyyan_liuzirui')

        url = 'http://datamechanics.io/data/_bps_transportation_challenge/buses.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        raw = json.loads(response)
        s = json.dumps(raw, sort_keys=True, indent=2)
        repo.dropCollection("bus_stop")
        repo.createCollection("bus_stop")

        coordinates = {}
        for i in raw['features']: 
            coordinates[i['properties']['number']] = i['geometry']['coordinates']



        for i in coordinates:
            new={}
            new[i]=coordinates[i]
            repo['yjunchoi_yzhang71_cyyan_liuzirui.bus_stop'].insert(new)
        # repo['yjunchoi_yzhang71_cyyan_liuzirui.bus_stop'].insert_many(r)
        #print(r)
        #repo['yjunchoi_yzhang71_cyyan_liuzirui.bus_stop'].metadata({'complete':True})
        #print(repo['yjunchoi_yzhang71_cyyan_liuzirui.bus_stop'].metadata())


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
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui','yjunchoi_yzhang71_cyyan_liuzirui')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/big-belly-locations/resource/15e7fa44-b9a8-42da-82e1-304e43460095')

        this_script = doc.agent('alg:yjunchoi_yzhang71_cyyan_liuzirui#bus_stop', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#bus_stop', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_bus_stop = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bus_stop, this_script)
        doc.usage(get_bus_stop, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        bus_stop = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#bus_stop', {prov.model.PROV_LABEL:'bus_stop', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bus_stop, this_script)
        doc.wasGeneratedBy(bus_stop, get_bus_stop, endTime)
        doc.wasDerivedFrom(bus_stop, resource, get_bus_stop, get_bus_stop, get_bus_stop)

        repo.logout()
                  
        return doc

bus_stop.execute()
doc = bus_stop.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
