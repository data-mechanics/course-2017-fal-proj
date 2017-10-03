import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class Station_Node(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = []
    writes = ['yjunchoi_yzhang71.Station_Node']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        url = 'http://datamechanics.io/data/yjunchoi_yzhang71/Station_Node.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Station_Node")
        repo.createCollection("Station_Node")

        
        for key in r:
            repo['yjunchoi_yzhang71.Station_Node'].insert_many(r[key])
         #   for key in r[row]: 
          #      print(r[row][key])
           #     print("hi")
            #Node = {}
            #Node[key] = r[key]
            #repo['yjunchoi_yzhang71.Station_Node'].insert(Node)

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

        this_script = doc.agent('alg:yjunchoi_yzhang71#Station_Node', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Station_Node.json', {'prov:label':'Station_Node', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Station_Node = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Station_Node, this_script)
        doc.usage(get_Station_Node, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Station + Node& $ select = id , name'
                  }
                  )

        Station_Node = doc.entity('dat:yjunchoi_yzhang71#Station_Node', {prov.model.PROV_LABEL:'Station_Node in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Station_Node, this_script)
        doc.wasGeneratedBy(Station_Node, get_Station_Node, endTime)
        doc.wasDerivedFrom(Station_Node, resource, get_Station_Node, get_Station_Node, get_Station_Node)

        repo.logout()

        return doc

#Station_Node.execute()
#doc = Station_Node.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
