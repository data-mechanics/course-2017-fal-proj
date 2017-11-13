import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from builtins import staticmethod

class underageSchoolsFiltered(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.underageSchools', 'bohorqux_peterg04_rocksdan_yfchen.colleges']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.underageSchoolsFiltered']
    
    @staticmethod
    def execute(trial = False):
        # helper functions from lecture 591 by Lapets
        def select(R, s):
            return [t for t in R if s(t)]
        
        def project(R, p):
            return [p(t) for t in R]
        
        def product(R, S):
            return [(t,u) for t in R for u in S]
        
        def union(R, S, I, B):
            return R + S + I + B
        
        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k,v) in R if k == key])) for key in keys]
        startTime = datetime.datetime.now()
        
        # Set up the db connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        
#        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.geojson'
#        response = urllib.request.urlopen(url).read().decode("utf-8")
#        r = json.loads(response)
#         s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("underageSchoolsFiltered")
        repo.createCollection("underageSchoolsFiltered")
        
        # set trafficData to a variable for manipulation
        initialSchoolData = repo[underageSchoolsFiltered.reads[0]].find()  # a list of dictionaries
        initialCollegeData = repo[underageSchoolsFiltered.reads[1]].find()
        
        tempSchoolData = select(initialSchoolData, lambda t: "properties" in t['features'][0])
        #print(tempSchoolData)
        
        
        
        zipcodeEntry = []
        addressEntry = []
        schoolID = []
        geometry = []
        
        for i in range(0, 132):
            ''' iterate through entire dataset '''
            a = project(tempSchoolData, lambda entry: ("ZIPCODE_ps", entry['features'][i]['properties']['ZIPCODE']) )
            b = project(tempSchoolData, lambda entry: ("ADDRESS", entry['features'][i]['properties']['ADDRESS']) )
            c = project(tempSchoolData, lambda entry: ("SCH_ID", entry['features'][i]['properties']['SCH_ID']) )
            d = project(tempSchoolData, lambda entry: ("coordinates", entry['features'][i]['geometry']['coordinates']) )
            
            zipcodeEntry += [a]
            addressEntry += [b]
            schoolID += [c]
            geometry += [d]
            
        finalSCHOOL = []
        finalSCHOOLtemp = []
        for i in range(0, 132):
            finalSCHOOL += [union(zipcodeEntry[i], addressEntry[i], schoolID[i],geometry[i])]
            finalSCHOOLtemp += [zipcodeEntry[i] + [1]]
        
        MergeDataSCHOOL = aggregate(project(finalSCHOOLtemp, lambda x: (x[0][1], x[1])), sum)
        
            
        
        #finalSchool will be an intermediate database if anyone needs to pull shoolID and more detailed lcoation for public schools
        
        #College Data
        
        
        tempCollegeData = select(initialCollegeData, lambda t: "properties" in t['features'][0])
        
        zipcodeEntryC = []
        addressEntryC = []
        schoolIDC = []
        geometryC = []
        
        for i in range(0, 60):
            ''' iterate through entire dataset '''
            a = project(tempCollegeData, lambda entry: ("ZIPCODE_c", entry['features'][i]['properties']['Zipcode']) )
            b = project(tempCollegeData, lambda entry: ("ADDRESS", entry['features'][i]['properties']['Address']) )
            c = project(tempCollegeData, lambda entry: ("SCH_ID", entry['features'][i]['properties']['SchoolId']) )
            d = project(tempCollegeData, lambda entry: ("coordinates", entry['features'][i]['geometry']['coordinates']) )
            
            zipcodeEntryC += [a]
            addressEntryC += [b]
            schoolIDC += [c]
            geometryC += [d]
        
        finalC = []
        finalCtemp = []
        for i in range(0, 60):
            finalC += [union(zipcodeEntryC[i], addressEntryC[i], schoolIDC[i],geometryC[i])]
            finalCtemp += [zipcodeEntry[i] + [1]]
        
        MergeDataC = aggregate(project(finalCtemp, lambda x: (x[0][1], x[1])), sum)
        
        
        #finalC will be an intermediate database if anyone needs to pull schoolID and more detailed location for colleges
        
        #Final Step: Merge two datas and find aggregate
        
        firstMerge = product(MergeDataC, MergeDataSCHOOL)
        selectMerge = select(firstMerge, lambda t: t[0][0] == t[1][0])
        
        projectMerge = project(selectMerge, lambda t: (t[0][0], t[0][1], t[1][1]))
        
     
        
        FinalData = project(projectMerge, lambda t: dict([("ZIPCODE", t[0]), ("College_Agg", t[1]), ("PublicSchool_Agg", t[2])]))
        
        repo['bohorqux_peterg04_rocksdan_yfchen.underageSchoolsFiltered'].insert(FinalData, check_keys = False)
        repo['bohorqux_peterg04_rocksdan_yfchen.underageSchoolsFiltered'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.underageSchoolsFiltered'].metadata())
        
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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/')

        this_script = doc.agent('alg:peterg04_yfchen#underageSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        get_underageSchoolsFiltered = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_underageSchoolsFiltered, this_script)
        doc.usage(get_underageSchoolsFiltered, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        underageSchoolsFiltered= doc.entity('dat:peterg04_yfchen#underageSchoolsFiltered', {prov.model.PROV_LABEL:'Boston Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(underageSchoolsFiltered, this_script)
        doc.wasGeneratedBy(underageSchoolsFiltered, get_underageSchoolsFiltered, endTime)
        doc.wasDerivedFrom(underageSchoolsFiltered, resource, get_underageSchoolsFiltered, get_underageSchoolsFiltered, get_underageSchoolsFiltered)

        repo.logout()
                  
        return doc
        
        
        
    
    
