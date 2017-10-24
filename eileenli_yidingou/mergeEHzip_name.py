import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergeHospitalEntertainment(dml.Algorithm):
    contributor = 'yidingou'
    reads = ['eileenli_yidingou.averageDelay', 'eileenli_yidingou.ename']
    writes = ['eileenli_yidingou.mergeHospitalEntertainment_data']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_yidingou', 'eileenli_yidingou')

        # loads the collection
        EN = repo['eileenli_yidingou.Entertainment'].find()
        HOS = repo['eileenli_yidingou.hospitals'].find()

        temp1 = []
        temp2 = []
        zipcode = []
        ename = []
        hname = []

        for kv in EN:
            for i in kv:
                if i == 'zip' or i == 'businessname':
                    try:
                        ename.append({kv['zip']: kv['businessname']})
                        temp1.append(kv['zip'])                   
                    except:
                        pass
        temp1 = list(set(temp1))

        nename = []
        for x in ename:
            if x not in nename:
                nename.append(x)

        for i in HOS:
            #print(i)
            for key in i:
                #print(key)
                if key == 'location_zip' or key == 'name':
                    #print('11111111')
                    try:
                        hname.append({i['location_zip']: i['name']})
                        temp2.append(i['location_zip'])
                    except:
                        pass

        nhname = []
        for x in hname:
            if x not in nhname:
                nhname.append(x)

        temp2 = list(set(temp2))
        temp1 = temp1 + temp2
        temp1 = list(set(temp1))
        a={}
        for i in temp1:
            for kv in nename:
                if str(i) in kv:
                    if kv[str(i)] not in a.values():
                        a.setdefault(str(i), []).append(kv[str(i)])

        for i in temp1:
            for kv in nhname:
                if str(i) in kv:
                    if kv[str(i)] not in a.values():
                        a.setdefault(str(i), []).append(kv[str(i)])
                    


        zipcode.append(a)



    
                

        #zipcode = list(set(zipcode))
        #print(nename)
        #print(hname)
        #print(a)
        print(zipcode)
        Name = zipcode

        repo.dropCollection("EHzip_name")
        repo.createCollection("EHzip_name")

        repo['eileenli_yidingou.EHzip_name'].insert_many(Name)
        repo['eileenli_yidingou.EHzip_name'].metadata({'complete': True})
        print("Saved EHzip_name", repo['eileenli_yidingou.EHzip_name'].metadata())

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
        repo.authenticate('eileenli_yidingou', 'eileenli_yidingou')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#mergeHospitalEntertainment',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_ename = doc.entity('dat:eileenli_yidingou#ename',
                                             {'prov:label': 'ename',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_hname = doc.entity('dat:eileenli_yidingou#hname',
                                             {'prov:label': 'hname',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        getName = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getName, this_script)
        doc.usage(getName, resource_hname, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getName, resource_ename, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        Name = doc.entity('dat:eileenli_yidingou#EHzip_name',
                          {prov.model.PROV_LABEL: 'EHname',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(Name, this_script)
        doc.wasGeneratedBy(Name, getName, endTime)
        doc.wasDerivedFrom(Name, resource_hname, getName, getName, getName)
        doc.wasDerivedFrom(Name, resource_ename, getName, getName, getName)
        
        repo.logout()

        return doc

mergeHospitalEntertainment.execute()
doc = mergeHospitalEntertainment.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof