import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class roadComplaints(dml.Algorithm):
    contributor = 'alanbur_jcaluag'
    reads = ['alanbur_jcaluag.roadComplaints']
    writes = ['alanbur_jcaluag.roadComplaintsAgg']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
 
        DSet=[]

        collection=repo['alanbur_jcaluag.roadComplaints'].find()
        DSet=[]
        # keys = {r[0] for r in R}
        # [(key, f([v for (k,v) in R if k == key])) for key in keys]
        DSet=[
             
             {
                'Latitude': item['geometry']['coordinates'][0],
                'Longitude': item['geometry']['coordinates'][1],
                'UserType' : item['properties']['USERTYPE'],
                'UserType' : item['properties']['COMMENTS'],
                'Comments':item['properties']['COMMENTS'],
                'Status': item['properties']['STATUS']
                'Date' : item['properties']['REQUESTDATE'][:item['properties']['REQUESTDATE'].index(':')]:
                }
            
              for item in collection
        ]
        DSetByDate=[]
        dates=set()
        for item in DSet:
            dates.add(item['Date'])
        for date in dates:
            [DSetByDate.append({"Date": date,
                "Incidents":[item in DSet if item['Date']==date]
                })
            ]
            

        # print(DSet)
        print(len(DSetByDate))

        repo.dropCollection("roadComplaintsAgg")
        repo.createCollection("roadComplaintsAgg")
        repo['alanbur_jcaluag.roadComplaintsAgg'].insert_many(DSet)
        repo['alanbur_jcaluag.roadComplaintsAgg'].metadata({'complete':True})
        print(repo['alanbur_jcaluag.roadComplaintsAgg'].metadata())
        
        

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
        repo.authenticate('alanbur_jcaluag', 'alanbur_jcaluag')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:alanbur_jcaluag#roadComplaints', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_complaints, this_script)
        doc.usage(get_complaints, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'5bed19f1f9cb41329adbafbd8ad260e5_0.geojson'
                  }
                  )

        roadComplaints = doc.entity('dat:alanbur_jcaluag#bikeNetwork', {prov.model.PROV_LABEL:'Road Complaints', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(roadComplaints, this_script)
        doc.wasGeneratedBy(roadComplaints, get_complaints, endTime)
        doc.wasDerivedFrom(roadComplaints, resource, get_complaints, get_complaints, get_complaints)

        repo.logout()
                  
        return doc
roadComplaints.execute()