import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class ppf_crime(dml.Algorithm):
    contributor = 'gaudiosi_raykatz_nedg'
    reads = ["gaudiosi_raykatz_nedg.ppf", "gaudiosi_raykatz_nedg.crime"]
    writes = ['gaudiosi_raykatz_nedg.ppf_crime']

    @staticmethod
    def execute(trial = False):
        '''Merge zipcode info'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
   
        r = []
        pprasdasd=[]
        ppfdict={}
        ppf_data = list(repo.gaudiosi_raykatz_nedg.ppf.find({}))
        for region in ppf_data:
            for i in range(5):
                year_sum = 0
                for j in range(1,13):
                    year_sum += int(region[str(2012 + i) + "-"+str(j).zfill(2)])
                ppfdict["average_priceperfoot_" + str(2012+i)]=(year_sum / 12)
                pprasdasd.append(year_sum / 12)

        fcrime_dict={}
        crime1 = list(repo.gaudiosi_raykatz_nedg.crime.find({}))
        crime_dict0=[x for x in crime1 if x['district'] == 'Downtown&Charlestown']
        crime_dict1=[x for x in crime1 if x['district'] == 'East_Boston']
        crime_dict2=[x for x in crime1 if x['district'] == 'Roxbury']
        crime_dict3=[x for x in crime1 if x['district'] == 'Mattapan']
        crime_dict4=[x for x in crime1 if x['district'] == 'South_Boston']
        crime_dict5=[x for x in crime1 if x['district'] == 'Dorchester']
        crime_dict6=[x for x in crime1 if x['district'] == 'Brighton_Allston']
        crime_dict7=[x for x in crime1 if x['district'] == 'West_Roxbury']
        crime_dict8=[x for x in crime1 if x['district'] == 'Jamaica_Plain']
        crime_dict9=[x for x in crime1 if x['district'] == 'Hyde_Park']


        finaldict={}
        finaldict["Charlestown"]=(len(crime_dict0), 'ppf :'+str(pprasdasd[0]))
        finaldict["East_Boston"]=(len(crime_dict1), 'ppf :'+str(pprasdasd[1]))
        finaldict["Roxbury"]=(len(crime_dict2), 'ppf :'+str(pprasdasd[2]))
        finaldict["Mattapan"]=(len(crime_dict3), 'ppf :'+str(pprasdasd[3]))
        finaldict["South_Boston"]=(len(crime_dict4), 'ppf :'+str(pprasdasd[4]))
        finaldict["Dorchester"]=(len(crime_dict5), 'ppf :'+str(pprasdasd[5]))
        finaldict["Brighton_Allston"]=(len(crime_dict6), 'ppf :'+str(pprasdasd[6]))
        finaldict["West_Roxbury"]=(len(crime_dict7), 'ppf :'+str(pprasdasd[7]))
        finaldict["Jamaica_Plain"]=(len(crime_dict8), 'ppf :'+str(pprasdasd[8]))
        finaldict["Hyde_Park"]=(len(crime_dict9), 'ppf :'+str(pprasdasd[9]))

        r.append(crime_dict0)
        r.append(pprasdasd[0])
        r.append(crime_dict1)
        r.append(pprasdasd[1])
        r.append(crime_dict2)
        r.append(pprasdasd[2])
        r.append(crime_dict3)
        r.append(pprasdasd[3])
        r.append(crime_dict4)
        r.append(pprasdasd[4])
        r.append(crime_dict5)
        r.append(pprasdasd[5])
        r.append(crime_dict6)
        r.append(pprasdasd[6])
        r.append(crime_dict7)
        r.append(pprasdasd[7])
        r.append(crime_dict8)
        r.append(pprasdasd[8])
        r.append(crime_dict9)
        r.append(pprasdasd[39])
        print(finaldict)



        # for i in range(6):
        #     fcrime_dict=+fcrime_dict+i


        # # ppf_data=select(ppf_data,'year'>=2012-00)
        # print(crime_dict1)
        # print(fcrime_dict)
        # print(crime_dict1)
        s = json.dumps(finaldict, sort_keys=True, indent=2)
        repo.dropCollection("ppf_crime")
        repo.createCollection("ppf_crime")
        repo['gaudiosi_raykatz_nedg.ppf_crime'].insert(finaldict)
        repo['gaudiosi_raykatz_nedg.ppf_crime'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz_nedg.ppf_crime'].metadata())
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
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:gaudiosi_raykatz_nedg#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)
        
        doc.usage(get_demos, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=ppf_crime&$select=stuff'
                  }
                  )
        
        demos = doc.entity('dat:gaudiosi_raykatz_nedg#ppf_crime', {prov.model.PROV_LABEL:'PPF Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()
                  
        return doc

'''
ppf_crime.execute()
doc = ppf_crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
