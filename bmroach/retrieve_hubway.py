import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from zipfile import ZipFile
from io import BytesIO


"""
Development notes:
Optimization potential in using PyMongo "insert_many" rather than making a db
call for every row with "insert_one". 
Reason it was done with insert_one: trouble with input type for insert_many
"""

class retrieve_hubway(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.hubway']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        hubwayDataSets = {
                      "2011": "https://s3.amazonaws.com/hubway-data/hubway_Trips_2011.csv",
                      "2012": "https://s3.amazonaws.com/hubway-data/hubway_Trips_2012.csv",
                      "2013": "https://s3.amazonaws.com/hubway-data/hubway_Trips_2013.csv",
                      "2014_1": "https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_1.csv",
                      "2014_2": "https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_2.csv",
                      
                      "2015_1": "https://s3.amazonaws.com/hubway-data/201501-hubway-tripdata.zip",
                      "2015_2": "https://s3.amazonaws.com/hubway-data/201502-hubway-tripdata.zip",
                      "2015_3": "https://s3.amazonaws.com/hubway-data/201503-hubway-tripdata.zip",
                      "2015_4": "https://s3.amazonaws.com/hubway-data/201504-hubway-tripdata.zip",
                      "2015_5": "https://s3.amazonaws.com/hubway-data/201505-hubway-tripdata.zip",
                      "2015_6": "https://s3.amazonaws.com/hubway-data/201506-hubway-tripdata.zip",
                      "2015_7": "https://s3.amazonaws.com/hubway-data/201507-hubway-tripdata.zip",
                      "2015_8": "https://s3.amazonaws.com/hubway-data/201508-hubway-tripdata.zip",
                      "2015_9": "https://s3.amazonaws.com/hubway-data/201509-hubway-tripdata.zip",
                      "2015_10": "https://s3.amazonaws.com/hubway-data/201510-hubway-tripdata.zip",
                      "2015_11": "https://s3.amazonaws.com/hubway-data/201511-hubway-tripdata.zip",
                      "2015_12": "https://s3.amazonaws.com/hubway-data/201512-hubway-tripdata.zip",

                      "2016_1": "https://s3.amazonaws.com/hubway-data/201601-hubway-tripdata.zip",
                      "2016_2": "https://s3.amazonaws.com/hubway-data/201602-hubway-tripdata.zip",
                      "2016_3": "https://s3.amazonaws.com/hubway-data/201603-hubway-tripdata.zip",
                      "2016_4": "https://s3.amazonaws.com/hubway-data/201604-hubway-tripdata.zip",
                      "2016_5": "https://s3.amazonaws.com/hubway-data/201605-hubway-tripdata.zip",
                      "2016_6": "https://s3.amazonaws.com/hubway-data/201606-hubway-tripdata.zip",
                      "2016_7": "https://s3.amazonaws.com/hubway-data/201607-hubway-tripdata.zip",
                      "2016_8": "https://s3.amazonaws.com/hubway-data/201608-hubway-tripdata.zip",
                      "2016_9": "https://s3.amazonaws.com/hubway-data/201609-hubway-tripdata.zip",
                      "2016_10": "https://s3.amazonaws.com/hubway-data/201610-hubway-tripdata.zip",
                      "2016_11": "https://s3.amazonaws.com/hubway-data/201611-hubway-tripdata.zip",

                      "2017_1": "https://s3.amazonaws.com/hubway-data/201701-hubway-tripdata.zip",
                      "2017_2": "https://s3.amazonaws.com/hubway-data/201702-hubway-tripdata.zip",
                      "2017_3": "https://s3.amazonaws.com/hubway-data/201703-hubway-tripdata.zip",
                      "2017_4": "https://s3.amazonaws.com/hubway-data/201704-hubway-tripdata.zip",
                      "2017_5": "https://s3.amazonaws.com/hubway-data/201705-hubway-tripdata.zip",
                      "2017_6": "https://s3.amazonaws.com/hubway-data/201706-hubway-tripdata.zip",
                      "2017_7": "https://s3.amazonaws.com/hubway-data/201707-hubway-tripdata.zip",
                      "2017_8": "https://s3.amazonaws.com/hubway-data/201708-hubway-tripdata.zip" 
                      }

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        
        # Do retrieving of data
        tripCount = 0          
        for timeFrame, url in hubwayDataSets.items(): #for every data set
            try:
                response = urllib.request.urlopen(url).read().decode("utf-8")
            except UnicodeDecodeError: #assume file read was a zip file
                res  = urllib.request.urlopen(url).read()
                zf_response = ZipFile( BytesIO(res) )
                filename = url[-26:-3]+"csv"
                response = str(zf_response.open(filename).read())
            
            responseListForm = response.split("\n")

            for i in range(len(responseListForm)): #for every line in each data set
                lineDict={}
                if i == 0: #disregard column names
                    continue
                line = responseListForm[i]
                lineItems = line.split(',')
                if lineItems == ['']: #disregard empty lines
                    continue
                
                try:
                    fields = {
                            0: "Trip Duration (seconds)",
                            1: "Start Time and Date",
                            2: "Stop Time and Date",
                            3: "Start Station Name & ID",
                            4: "End Station Name & ID",
                            5: "Bike ID",
                            6: "User Type (Casual = 24-Hour or 72-Hour Pass user; Member = Annual or Monthly Member)",
                            7: "Zip Code, if member",
                            8: "Gender, self-reported by member"
                            }

                    #selected information from above added to db... 

                    for index, fieldName in fields.items():
                        lineDict[fieldName] = lineItems[index]
                
                except IndexError: #for catching non-standard lines in the data
                    print("Error caught. Current line: ",lineItems)

                #add line to db collection               
                repo["bmroach.hubway"].insert_one( {str(tripCount) : lineDict} )    
                tripCount += 1
          
            print(timeFrame, "dataset imported to db")

        repo['bmroach.hubway'].metadata({'complete':True})  
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
        repo.authenticate('bmroach', 'bmroach')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        repo.logout()
                  
        return doc





retrieve_hubway.execute()

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
