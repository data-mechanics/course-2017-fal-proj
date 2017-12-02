import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crime(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = []
    writes = ['raykatz_nedg_gaudiosi.crime']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')

        url = 'https://data.cityofboston.gov/resource/crime.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        result = json.loads(response)
        r=[]
        

        # for i in range(1,len(r)):
        #     d={}

        #     d['Downtown&Charlestown']=result[i]["A1"]
        #     d['Downtown&Charlestown']=result[i]["A15"]
        #     d['East Boston']=result[i]["A7"]
        #     d['Roxbury']=result[i]["B2"]
        #     d['Mattapan']=result[i]["B3"]
        #     d['South Boston']=result[i]['C6']
        #     d['Dorchester']=result[i]["C11"]
        #     d['South End']=result[i]["D4"]
        #     d['Brighton/Allston']=result[i]["D14"]
        #     d['West Roxbury']=result[i]["E5"]
        #     d['Jamaica Plain']=result[i][E13]
        #     d['Hyde Park']=result[i][E18]
        #     r.append(d)
        #     print(d)
        # print(r)
        district_dict = {"A1":"Downtown&Charlestown","A15":"Downtown&Charlestown","A7":"East_Boston","B2":'Roxbury',"B3":"Mattapan","C6":"South_Boston","C11":"Dorchester","D4":"South_End","D14":"Brighton_Allston","E5":"West_Roxbury","E13":"Jamaica_Plain","E18":"Hyde_Park"}
        for i in range(0,len(result)):
            d={}
            d['year']=result[i]["year"]
            d['district']=district_dict[result[i]["reptdistrict"]]

            r.append(d)



        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['raykatz_nedg_gaudiosi.crime'].insert_many(r)
        repo['raykatz_nedg_gaudiosi.crime'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.crime'].metadata())

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
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

      
        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:crime.json', {'prov:label':'Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )


        crime = doc.entity('dat:raykatz_nedg_gaudiosi#crime', {prov.model.PROV_LABEL:'Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()
                  
        return doc
'''
crime.execute()
doc = crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
