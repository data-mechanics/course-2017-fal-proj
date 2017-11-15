import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class income(dml.Algorithm):
    contributor = 'raykatz_nedg_gaudiosi'
    reads = []
    writes = ['raykatz_nedg_gaudiosi.income']

    @staticmethod
    def execute(trial = False):
        '''Retrieve income and income data from US Census'''
        startTime = datetime.datetime.now()
        trial_zips = ["02116", "02134", "02215"]

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        url = "https://api.census.gov/data/2015/acs5?get=B19013_001E,B25070_010E,B25070_001E,B25111_001E,B17023_002E,B17023_001E&for=zip+code+tabulation+area:02021,02108,02109,02110,02111,02112,02113,02114,02115,02116,02117,02118,02119,02120,02121,02122,02123,02124,02125,02126,02127,02128,02129,02130,02131,02132,02133,02134,02135,02136,02137,02163,02196,02199,02201,02203,02204,02205,02206,02207,02210,02211,02212,02215,02216,02217,02222,02228,02241,02266,02283,02284,02293,02295,02297,02298,02459,02151,02186,02026,02152,02467&key="
        with open('auth.json') as data_file:    
                data = json.load(data_file)
        url += data["census"]
        
        #Returns the ordered by population numbers of [median income, total spending 50%+ income on rent, total renters, median rent, people in poverty, total people, zipcode]
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        result = json.loads(response)
        r = []
        for i in range(1,len(result)):
            if int(result[i][5]) == 0:
                continue
            #if result[i][0] == None or int(result[i][2]) == 0 or result[i][3] == None or int(result[i][5]) == 0:
            #    continue
            zipcode = result[i][6]
            if trial and zipcode not in trial_zips:
                continue
            d = {}
            d["median_income"] = int(result[i][0]) if not result[i][0] == None else 0
            d["50_income_rent"] = int(result[i][1])
            d["total_renters"] = int(result[i][2])
            d["median_rent"] = int(result[i][3]) if not result[i][3] == None else 0
            d["people_in_poverty"] = int(result[i][4]) 
            d["total_people"] = int(result[i][5])
            d["zipcode"] = zipcode
            r.append(d)
        
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("income")
        repo.createCollection("income")
        repo['raykatz_nedg_gaudiosi.income'].insert_many(r)
        repo['raykatz_nedg_gaudiosi.income'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.income'].metadata())
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
        doc.add_namespace('census', 'https://api.census.gov/data/')

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('census:2015', {'prov:label':'Income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income, this_script)
        
        doc.usage(get_income, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'/acs5?get=B19013_001E,B25070_010E,B25070_001E,B25111_001E,B17023_002E,B17023_001E&for=zip+code+tabulation+area:02021,02108,02109,02110,02111,02112,02113,02114,02115,02116,02117,02118,02119,02120,02121,02122,02123,02124,02125,02126,02127,02128,02129,02130,02131,02132,02133,02134,02135,02136,02137,02163,02196,02199,02201,02203,02204,02205,02206,02207,02210,02211,02212,02215,02216,02217,02222,02228,02241,02266,02283,02284,02293,02295,02297,02298,02459,02151,02186,02026,02152,02467'
                  }
                  )
        
        income = doc.entity('dat:raykatz_nedg_gaudiosi#income', {prov.model.PROV_LABEL:'Income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, get_income, endTime)
        doc.wasDerivedFrom(income, resource, get_income, get_income, get_income)

        repo.logout()
                  
        return doc
'''
income.execute()
doc = income.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
