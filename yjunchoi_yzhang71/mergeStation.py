import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergeStation(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71'
    reads = ['yjunchoi_yzhang71.averageDelay', 'yjunchoi_yzhang71.Station_Node']
    writes = ['yjunchoi_yzhang71.mergeStation_data']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')

        # loads the collection
        AD = repo['yjunchoi_yzhang71.averageDelay'].find()
        SN = repo['yjunchoi_yzhang71.Station_Node'].find()
        
        # projection
        delayTime = []
        for i in AD:
            for key in i:
                if(key != "_id"):
                    abbList = key.split("|")
                    #print(abbList)
                    SN = repo['yjunchoi_yzhang71.Station_Node'].find()
                    for j in SN:
                        if abbList[0] == j['id']:
                            #print(j['name'])
                            abbList[0] = j['name']
                            #print(abbList[0])
                        if abbList[1] == j['id']:
                            abbList[1] = j['name']
                        name = abbList[0] + "|" + abbList[1]           
            try:
                delayTime.append({name:i[key]})

                
            except:
                pass
        #print(delayTime)

        stationNode = []
        for key in SN:
            try:
                stationNode.append({key['id']: key['name']})
            except:
                pass

        # aggregation
        
        stationData = delayTime
        #print(stationData)
        #print(stationNode)
        #print(delayTime)
        #print(stationData)

        repo.dropCollection("Station_data")
        repo.createCollection("Station_data")

        repo['yjunchoi_yzhang71.Station_data'].insert_many(stationData)
        repo['yjunchoi_yzhang71.Station_data'].metadata({'complete': True})
        print("Saved station_data", repo['yjunchoi_yzhang71.station_data'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71', 'yjunchoi_yzhang71')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#mergeStation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_delayTime = doc.entity('dat:yjunchoi_yzhang71#delayTime',
                                             {'prov:label': 'delayTime',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_stationNode = doc.entity('dat:yjunchoi_yzhang71#Station_Node',
                                             {'prov:label': 'Station_Node',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        get_stationData = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stationData, this_script)
        doc.usage(get_stationData, resource_delayTime, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_stationData, resource_stationNode, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        stationData = doc.entity('dat:yjunchoi_yzhang71#station_data',
                          {prov.model.PROV_LABEL: 'Station Data',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stationData, this_script)
        doc.wasGeneratedBy(stationData, get_stationData, endTime)
        doc.wasDerivedFrom(stationData, resource_delayTime, get_stationData, get_stationData, get_stationData)
        doc.wasDerivedFrom(stationData, resource_stationNode, get_stationData, get_stationData, get_stationData)
        
        repo.logout()

        return doc

mergeStation.execute()
doc = mergeStation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof