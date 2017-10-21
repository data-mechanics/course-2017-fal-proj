import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class Station_Links(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = []
    writes = ['yjunchoi_yzhang71.Station_Links']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        url = 'http://datamechanics.io/data/yjunchoi_yzhang71/Station_Links.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Station_Links")
        repo.createCollection("Station_Links")

        
        for key in r:
            repo['yjunchoi_yzhang71.Station_Links'].insert_many(r[key])
         #   for key in r[row]: 
          #      print(r[row][key])
           #     print("hi")
            #Node = {}
            #Node[key] = r[key]
            #repo['yjunchoi_yzhang71.Station_Links'].insert(Node)

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
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/yjunchoi_yzhang71') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/yjunchoi_yzhang71') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dat', 'http://datamechanics.io/data/yjunchoi_yzhang71/')

        this_script = doc.agent('alg:yjunchoi_yzhang71#Station_Links', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Station_Links.json', {'prov:label':'Station_Links', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Station_Links = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Station_Links, this_script)
        doc.usage(get_Station_Links, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Station + Links& $ select = source, target, line, color'
                  }
                  )

        Station_Links = doc.entity('dat:yjunchoi_yzhang71#Station_Links', {prov.model.PROV_LABEL:'Station_Links in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Station_Links, this_script)
        doc.wasGeneratedBy(Station_Links, get_Station_Links, endTime)
        doc.wasDerivedFrom(Station_Links, resource, get_Station_Links, get_Station_Links, get_Station_Links)

        repo.logout()

        return doc

#Station_Links.execute()
#doc = Station_Links.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
