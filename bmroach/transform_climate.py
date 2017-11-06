import urllib.request
import json
import dml, prov.model
import datetime, uuid
import geojson

"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

Aggregate Hubway trips by Boston neighborgood

Development notes:


"""

class transform_climate(dml.Algorithm):
    contributor = 'bmroach'
    reads = ['bmroach.boston_LCD']
    writes = []

    @staticmethod
    def execute(trial = False, log=False):
        startTime = datetime.datetime.now()
        

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        #Read in  data
        idVal = 0
        climate = repo.bmroach.boston_LCD.find()
        preferredList = []
        for entry in climate:
            entry = entry[str(idVal)]
            idVal += 1

            if entry["HOURLYRelativeHumidity"] == '' or entry['HOURLYWindSpeed'] == '' or entry["HOURLYDRYBULBTEMPF"] == '':
                continue

            if int(entry["HOURLYRelativeHumidity"]) < 70:
                if int(entry['HOURLYWindSpeed']) < 15:
                    if int(entry["HOURLYDRYBULBTEMPF"]) > 50 and int(entry["HOURLYDRYBULBTEMPF"]) < 85: 
                        d = entry["DATE"][:10]
                        if d not in preferredList:                    
                            preferredList.append(d)
                    

        goal = {}      
        for day in preferredList:
            month=int(day[5:7])
            
            if month in goal:
                goal[month] += 1

            else:
                goal[month] = 1
            
            
    
        with open("./custom_output_datasets/desirable_weather.json", 'w') as outfile:
            json.dump(goal, outfile)

        
        
        
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

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.        

        this_script = doc.agent('alg:bmroach#transform_climate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('dat:bmroach#boston_LCD', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        transform = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(transform, this_script)
        
        doc.usage(transform,resource, startTime, None,
                            {prov.model.PROV_TYPE:'ont:Retrieval',
                            'ont:Query':''  
                            }
                            )


        desirable_weather = doc.entity('dat:bmroach#desirable_weather', {prov.model.PROV_LABEL:'desirable_weather', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(desirable_weather, this_script)
        doc.wasGeneratedBy(desirable_weather, transform, endTime)
        
        doc.wasDerivedFrom(desirable_weather, resource, transform, transform, transform)
      
        repo.logout()                  
        return doc




# transform_climate.execute(log=True)

# doc = transform_open_space.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
