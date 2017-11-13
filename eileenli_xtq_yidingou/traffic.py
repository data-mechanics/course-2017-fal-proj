import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict



class traffic(dml.Algorithm):
    contributor = 'eileenli_xtq_yidingou'
    reads = ['eileenli_xtq_yidingou.crash', 'eileenli_xtq_yidingou.hubway', 'eileenli_xtq_yidingou.signals']
    writes = ['eileenli_xtq_yidingou.traffic']


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
        CR = repo['eileenli_xtq_yidingou.crash'].find() #Entertainment
        HU = repo['eileenli_xtq_yidingou.hubway'].find() #Restaurants
        SI = repo['eileenli_xtq_yidingou.signals'].find()


        cr_cord = []
        for k in CR:
            try:
                cr_cord.append([float(k['longitude']), float(k['latitude'])])
            except:
                pass

        hu_cord = []
        for k in HU:
            try:
                hu_cord.append(k['geometry']['coordinates'])
            except:
                pass


        si_cord = []
        for k in SI:
            try:
                si_cord.append(k['geometry']['coordinates'])
            except:
                pass

        
        all_cord = dict([("crash", cr_cord), ("hubway", hu_cord), ("signals", si_cord)])


        repo.dropCollection("traffic")
        repo.createCollection("traffic")

        repo['eileenli_xtq_yidingou.traffic'].insert_one(all_cord)
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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<fileEntRes> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#traffic', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_crash = doc.entity('dat:eileenli_xtq_yidingou#crash', {'prov:label': 'crash', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_hubway = doc.entity('dat:eileenli_xtq_yidingou#hubway', {'prov:label': 'hubway', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_signals = doc.entity('dat:eileenli_xtq_yidingou#signals', {'prov:label': 'signals', prov.model.PROV_TYPE: 'ont:DataSet'})
        

        get_EntRes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_EntRes, this_script)
        doc.usage(get_EntRes, resource_crash, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_EntRes, resource_hubway, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_EntRes, resource_signals, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        

        EntRes = doc.entity('dat:eileenli_xtq_yidingou#traffic', {prov.model.PROV_LABEL: 'traffic situation', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(EntRes, this_script)
        doc.wasGeneratedBy(EntRes, get_EntRes, endTime)
        doc.wasDerivedFrom(EntRes, resource_crash, get_EntRes, get_EntRes, get_EntRes)
        doc.wasDerivedFrom(EntRes, resource_hubway, get_EntRes, get_EntRes, get_EntRes)
        doc.wasDerivedFrom(EntRes, resource_signals, get_EntRes, get_EntRes, get_EntRes)
        
        repo.logout()

        return doc

traffic.execute()
doc = traffic.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof