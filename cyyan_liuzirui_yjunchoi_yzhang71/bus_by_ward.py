from collections import Counter
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import matplotlib.path as mplPath
import numpy as np
import pandas as pd

class bus_by_ward(dml.Algorithm):
    contributor = 'yjunchoi_yzhang71_cyyan_liuzirui'
    reads = ['yjunchoi_yzhang71_cyyan_liuzirui.wards', 'yjunchoi_yzhang71_cyyan_liuzirui.busstopCoordinates']
    writes = ['yjunchoi_yzhang71_cyyan_liuzirui.bus_by_ward']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('yjunchoi_yzhang71_cyyan_liuzirui','yjunchoi_yzhang71_cyyan_liuzirui')

        # loads ward coordinate
        raw_ward = repo['yjunchoi_yzhang71_cyyan_liuzirui.wards'].find({})

        # loads bus stop data
        raw_bus = repo['yjunchoi_yzhang71_cyyan_liuzirui.busstopCoordinates'].find()

        # print("hi")
        # Collection ward coordinate
        path = {}
        for i in raw_ward:
            #print("hi", i['coordinates'][0][0])
            if isinstance(i['coordinates'][0][0], list):
                #print("hi", i['coordinates'][0])
                path[i['ward_num']] = mplPath.Path(i['coordinates'][0])
            else:
                path[i['ward_num']] = mplPath.Path(i['coordinates'])
        
        bStop = pd.DataFrame(list(raw_bus))
        bStop['coordinates'] = bStop.coordinates      

        #print("i", path)

        raw_ward = repo['yjunchoi_yzhang71_cyyan_liuzirui.wards'].find({})

        busByWard = {}
        for j in raw_ward:
            # print("?", i)
            coordinate = []
            for i in range(len(bStop['coordinates'])):
                #print(path[j['ward_num']].contains_point(bStop['coordinates'][i]))
                #print(j['ward_num'])
                if path[j['ward_num']].contains_point(bStop['coordinates'][i]):
                    coordinate.append(bStop['coordinates'][i])
                    #print("hi")
            #print(coordinate)    
            busByWard[str(j['ward_num'])] = coordinate

        #print("!!!!!!", busByWard)
        results = [busByWard]
        # drop collection amount_of_police_hospital
        repo.dropCollection('bus_by_ward')
        # create collection amount_of_police_hospital
        repo.createCollection('bus_by_ward')
        # insert data in collection amount_of_police_hospital
        repo['yjunchoi_yzhang71_cyyan_liuzirui.bus_by_ward'].insert(results)

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

        #create document object and define namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/')
        doc.add_namespace('hpa', 'https://data.boston.gov/')
        
        #define entity to represent resources
        this_script = doc.agent('alg:yjunchoi_yzhang71_cyyan_liuzirui#amount_of_police_hospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#police', {prov.model.PROV_LABEL:'police', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        
        ph = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(ph, this_script)

        doc.usage(ph, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(ph, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        p = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#police', {prov.model.PROV_LABEL:'police stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(p, this_script)
        doc.wasGeneratedBy(p, ph, endTime)
        doc.wasDerivedFrom(p, resource1, ph, ph, ph)

        h = doc.entity('dat:yjunchoi_yzhang71_cyyan_liuzirui#hospital', {prov.model.PROV_LABEL:'hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(h, this_script)
        doc.wasGeneratedBy(h, ph, endTime)
        doc.wasDerivedFrom(h, resource2, ph, ph, ph)

        repo.logout()
                  
        return doc

bus_by_ward.execute()
doc = bus_by_ward.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
