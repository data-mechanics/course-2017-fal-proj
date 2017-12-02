import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict



class comfort(dml.Algorithm):
    contributor = 'eileenli_xtq_yidingou'
    reads = ['eileenli_xtq_yidingou.Entertainment', 'eileenli_xtq_yidingou.Restaurants']
    writes = ['eileenli_xtq_yidingou.comfort']


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
        EN = repo['eileenli_xtq_yidingou.Entertainment'].find()
        RS = repo['eileenli_xtq_yidingou.Restaurants'].find()



        en_cord = []
        l = []
        for k in EN:
            for i in k:
                if i == 'businessname' or i == 'location':
                    try:
                        nt = k['location'].strip("()").split(",")
                        l = [(float(nt[1]),float(nt[0]))]
                        en_cord += l
                    except:
                        pass

        rs_cord = []
        for k in RS:
            for i in k:
                if i == 'location':
                    try:
                        rs_cord += [tuple(k['location']['coordinates'])]
                    except:
                        pass
        result = []
        for i in rs_cord:
            if i[0]==0 and i[1]==0:
                result = result
            else:
                result.append(i)

        all_cord = dict([("entertainment",en_cord),("restaurants",(result))])


        repo.dropCollection("comfort")
        repo.createCollection("comfort")

        repo['eileenli_xtq_yidingou.comfort'].insert_one(all_cord)


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

        this_script = doc.agent('alg:#comfort', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_entertainments = doc.entity('dat:eileenli_xtq_yidingou#entertainments', {'prov:label': 'entertainments', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_restaurants = doc.entity('dat:eileenli_xtq_yidingou#restaurants', {'prov:label': 'restaurants', prov.model.PROV_TYPE: 'ont:DataSet'})
        

        get_EntRes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_EntRes, this_script)
        doc.usage(get_EntRes, resource_entertainments, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_EntRes, resource_restaurants, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        

        EntRes = doc.entity('dat:eileenli_xtq_yidingou#comfort', {prov.model.PROV_LABEL: 'Entertainment Restaurants', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(EntRes, this_script)
        doc.wasGeneratedBy(EntRes, get_EntRes, endTime)
        doc.wasDerivedFrom(EntRes, resource_entertainments, get_EntRes, get_EntRes, get_EntRes)
        doc.wasDerivedFrom(EntRes, resource_restaurants, get_EntRes, get_EntRes, get_EntRes)
        
        repo.logout()

        return doc

comfort.execute()
doc = comfort.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof