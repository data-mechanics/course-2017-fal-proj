import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict



class safety(dml.Algorithm):
    contributor = 'eileenli_xtq_yidingou'
    reads = ['eileenli_xtq_yidingou.hospitals', 'eileenli_xtq_yidingou.Crime','eileenli_xtq_yidingou.crash']
    writes = ['eileenli_xtq_yidingou.safety_data']


    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')

        # loads the collection
        HP = repo['eileenli_xtq_yidingou.hospitals'].find()
        CR = repo['eileenli_xtq_yidingou.Crime'].find()
        CA = repo['eileenli_xtq_yidingou.crash'].find()


        hp_cord = []
        for k in HP:
            for i in k:
                if i == 'location':
                    try:
                        hp_cord.append(tuple(k['location']['coordinates']))
                    except:
                        pass
        #print(hp_cord)

        crime_cord = []
        for key in CR:
            for i in key['location']:
                if i == 'coordinates':
                    try:
                        crime_cord.append(tuple(key['location']['coordinates']))
                        
                    except:
                        pass
        result = []
        for i in crime_cord:
            if i[0]==0 and i[1]==0:
                result = result
            else:
                result.append(i)
        #print(result)

        ca_cord = []
        for s in CA:
            for i in s:
                if i == 'longitude' or i == 'latitude':
                    try:
                        ca_cord.append(tuple([float(s['longitude']),float(s['latitude'])]))
                    except:
                        pass
        #print(dict([("hospitals",hp_cord)]))
        all_cord = dict([("hospitals",hp_cord),("crimes",(result)),("crash",(ca_cord))])
        #print(all_cord)


        repo.dropCollection("safety")
        repo.createCollection("safety")

        repo['eileenli_xtq_yidingou.safety'].insert_one(all_cord)
        setdis=[]

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
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filesafety> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#safety',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_hospitals = doc.entity('dat:eileenli_xtq_yidingou#hospitals',
                                             {'prov:label': 'hospitals',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_crime = doc.entity('dat:eileenli_xtq_yidingou#crime',
                                             {'prov:label': 'crime',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_crash = doc.entity('dat:eileenli_xtq_yidingou#crash',
                                             {'prov:label': 'crash',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        

        get_safety = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_safety, this_script)
        doc.usage(get_safety, resource_hospitals, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_safety, resource_crime, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_safety, resource_crash, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        

        safety = doc.entity('dat:eileenli_xtq_yidingou#safety',
                          {prov.model.PROV_LABEL: 'safety',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(safety, this_script)
        doc.wasGeneratedBy(safety, get_safety, endTime)
        doc.wasDerivedFrom(safety, resource_hospitals, get_safety, get_safety, get_safety)
        doc.wasDerivedFrom(safety, resource_crime, get_safety, get_safety, get_safety)
        doc.wasDerivedFrom(safety, resource_crash, get_safety, get_safety, get_safety)

        
        repo.logout()

        return doc

safety.execute()
doc = safety.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof