import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from zipfile import ZipFile
from io import BytesIO


"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

Hubway trip log

Development notes:

"""

class retrieve_hubway(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.hubway']

    @staticmethod
    def execute(trial = False, log=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        hubwayDataSets = {
                      "2011": ("https://s3.amazonaws.com/hubway-data/hubway_Trips_2011.csv", "OLD"),
                      "2012": ("https://s3.amazonaws.com/hubway-data/hubway_Trips_2012.csv", "OLD"),
                      "2013": ("https://s3.amazonaws.com/hubway-data/hubway_Trips_2013.csv", "OLD"),
                      "2014_1": ("https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_1.csv", "OLD"),
                      "2014_2": ("https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_2.csv", "OLD"),
                      
                      "2015_1": ("https://s3.amazonaws.com/hubway-data/201501-hubway-tripdata.zip", "NEW"),
                      "2015_2": ("https://s3.amazonaws.com/hubway-data/201502-hubway-tripdata.zip", "NEW"),
                      "2015_3": ("https://s3.amazonaws.com/hubway-data/201503-hubway-tripdata.zip", "NEW"),
                      "2015_4": ("https://s3.amazonaws.com/hubway-data/201504-hubway-tripdata.zip", "NEW"),
                      "2015_5": ("https://s3.amazonaws.com/hubway-data/201505-hubway-tripdata.zip", "NEW"),
                      "2015_6": ("https://s3.amazonaws.com/hubway-data/201506-hubway-tripdata.zip", "NEW"),
                      "2015_7": ("https://s3.amazonaws.com/hubway-data/201507-hubway-tripdata.zip", "NEW"),
                      "2015_8": ("https://s3.amazonaws.com/hubway-data/201508-hubway-tripdata.zip", "NEW"),
                      "2015_9": ("https://s3.amazonaws.com/hubway-data/201509-hubway-tripdata.zip", "NEW"),
                      "2015_10": ("https://s3.amazonaws.com/hubway-data/201510-hubway-tripdata.zip", "NEW"),
                      "2015_11": ("https://s3.amazonaws.com/hubway-data/201511-hubway-tripdata.zip", "NEW"),
                      "2015_12": ("https://s3.amazonaws.com/hubway-data/201512-hubway-tripdata.zip", "NEW"),

                      "2016_1": ("https://s3.amazonaws.com/hubway-data/201601-hubway-tripdata.zip", "NEW"),
                      "2016_2": ("https://s3.amazonaws.com/hubway-data/201602-hubway-tripdata.zip", "NEW"),
                      "2016_3": ("https://s3.amazonaws.com/hubway-data/201603-hubway-tripdata.zip", "NEW"),
                      "2016_4": ("https://s3.amazonaws.com/hubway-data/201604-hubway-tripdata.zip", "NEW"),
                      "2016_5": ("https://s3.amazonaws.com/hubway-data/201605-hubway-tripdata.zip", "NEW"),
                      "2016_6": ("https://s3.amazonaws.com/hubway-data/201606-hubway-tripdata.zip", "NEW"),
                      "2016_7": ("https://s3.amazonaws.com/hubway-data/201607-hubway-tripdata.zip", "NEW"),
                      "2016_8": ("https://s3.amazonaws.com/hubway-data/201608-hubway-tripdata.zip", "NEW"),
                      "2016_9": ("https://s3.amazonaws.com/hubway-data/201609-hubway-tripdata.zip", "NEW"),
                      "2016_10": ("https://s3.amazonaws.com/hubway-data/201610-hubway-tripdata.zip", "NEW"),
                      "2016_11": ("https://s3.amazonaws.com/hubway-data/201611-hubway-tripdata.zip", "NEW"),

                      "2017_1": ("https://s3.amazonaws.com/hubway-data/201701-hubway-tripdata.zip", "NEW"),
                      "2017_2": ("https://s3.amazonaws.com/hubway-data/201702-hubway-tripdata.zip", "NEW"),
                      "2017_3": ("https://s3.amazonaws.com/hubway-data/201703-hubway-tripdata.zip", "NEW"),
                      "2017_4": ("https://s3.amazonaws.com/hubway-data/201704-hubway-tripdata.zip", "NEW"),
                      "2017_5": ("https://s3.amazonaws.com/hubway-data/201705-hubway-tripdata.zip", "NEW"),
                      "2017_6": ("https://s3.amazonaws.com/hubway-data/201706-hubway-tripdata.zip", "NEW"),
                      "2017_7": ("https://s3.amazonaws.com/hubway-data/201707-hubway-tripdata.zip", "NEW"),
                      "2017_8": ("https://s3.amazonaws.com/hubway-data/201708-hubway-tripdata.zip", "NEW")
                      }

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        
        repo.dropCollection("hubway")
        repo.createCollection("hubway")
        
        # Do retrieving of data
        #Different Schemes depending on pre 2015 or not
        old = {
            0: 'Duration',
            1: 'Start Date',
            2: 'End Date',
            3: 'Start Station Number',
            4: 'Start Station Name',
            5: 'End Station Number',
            6: 'End Station Name',
            7: 'Bike Number',
            8: 'Member Type',
            9: 'Zip Code',
            10: 'Gender'
        }
        
        new = {
            0: 'Duration',
            1: 'Start Date',
            2: 'End Date',
            3: 'Start Station Number',
            4: 'Start Station Name',
            5: 'Start Station Latitude',
            6: 'Start Station Longitude',
            7: 'End Station Number',
            8: 'End Station Name',
            9: 'End Station Latitude',
            10: 'End Station Longitude',
            11: 'Bike Number',
            12: 'Member Type',
            13: 'Birth Year',
            14: 'Gender'
        }

        #Retrieve the Hubway Station Coordinates
        stationCoords = {}
        urlCoords = 'https://s3.amazonaws.com/hubway-data/Hubway_Stations_2011_2016.csv'
        responseCoords = urllib.request.urlopen(urlCoords).read().decode("utf-8")
        
        #For reference about rows in responseCoords...
        #indices={1: 'Station ID', 2: 'Latitude',3: 'Longitude'}
        
        rows = responseCoords.split('\n')
        for i in range(len(rows)):
            line = rows[i] 
            if line == '':
                continue
            line = line.split(',') #separate single line into item
            #Station ID becomes key            
            stationCoords[ line[1] ] = {
                                        'Latitude' : line[2],
                                        'Longitude': line[3]
                                    }            

        #Constructing the list of dicts to add to mongo collection
        tripCount = 0
        hubwayList = []          
        for timeFrame, (url, scheme) in hubwayDataSets.items(): #for every data set
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
                
                if scheme == 'OLD':
                    thisScheme = old

                elif scheme == "NEW":
                    thisScheme = new

                
                for index, fieldName in thisScheme.items():
                    lineDict[fieldName] = lineItems[index]
            
                #if OLD, need to look up latitude and longitude
                if scheme == "OLD":
                    start = lineDict['Start Station Number']
                    end = lineDict['End Station Number']

                    try:
                        startCoord = stationCoords[start]                    
                        lineDict['Start Station Latitude'] = startCoord['Latitude']
                        lineDict['Start Station Longitude'] = startCoord['Longitude']                    
                    except KeyError:
                        lineDict['Start Station Latitude'] = "0"
                        lineDict['Start Station Longitude'] = "0"

                    try:
                        endCoord = stationCoords[end]
                        lineDict['End Station Latitude'] = endCoord['Latitude']
                        lineDict['End Station Longitude'] = endCoord['Longitude']                    
                    except KeyError:
                        lineDict['End Station Latitude'] = "0"
                        lineDict['End Station Longitude'] = "0"

                #add line to db collection               
                hubwayList.append( {str(tripCount) : lineDict} )    
                tripCount += 1
            if log:
                print(timeFrame, "dataset imported to db")

        repo["bmroach.hubway"].insert_many( hubwayList )    
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
        doc.add_namespace('hbw', 'https://s3.amazonaws.com/hubway-data/')

        this_script = doc.agent('alg:bmroach#retrieve_hubway', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource0 = doc.entity('hbw:hubway_Trips_2011', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource1 = doc.entity('hbw:hubway_Trips_2012', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource2 = doc.entity('hbw:hubway_Trips_2013', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource3 = doc.entity('hbw:hubway_Trips_2014_1', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource4 = doc.entity('hbw:hubway_Trips_2014_2', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

        resource5 = doc.entity('hbw:201501-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource6 = doc.entity('hbw:201502-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource7 = doc.entity('hbw:201503-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource8 = doc.entity('hbw:201504-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource9 = doc.entity('hbw:201505-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource10 = doc.entity('hbw:201506-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource11 = doc.entity('hbw:201507-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource12 = doc.entity('hbw:201508-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource13 = doc.entity('hbw:201509-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource14 = doc.entity('hbw:201510-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource15 = doc.entity('hbw:201511-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource16 = doc.entity('hbw:201512-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})

        resource17 = doc.entity('hbw:201601-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource18 = doc.entity('hbw:201602-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource19 = doc.entity('hbw:201603-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource20 = doc.entity('hbw:201604-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource21 = doc.entity('hbw:201605-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource22 = doc.entity('hbw:201606-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource23 = doc.entity('hbw:201607-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource24 = doc.entity('hbw:201608-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource25 = doc.entity('hbw:201609-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource26 = doc.entity('hbw:201610-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource27 = doc.entity('hbw:201611-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource28 = doc.entity('hbw:201612-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})

        resource29 = doc.entity('hbw:201701-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource30 = doc.entity('hbw:201702-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource31 = doc.entity('hbw:201703-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource32 = doc.entity('hbw:201704-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource33 = doc.entity('hbw:201705-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource34 = doc.entity('hbw:201706-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource35 = doc.entity('hbw:201707-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        resource36 = doc.entity('hbw:201708-hubway-tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
        
        get_hubway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_hubway, this_script)
        
        for i in range(37):
            command = """doc.usage(get_hubway,resource"""+str(i)+""", startTime, None,
                                {prov.model.PROV_TYPE:'ont:Retrieval',
                                'ont:Query':''  
                                }
                                )"""
            exec(command)

        hubway = doc.entity('dat:bmroach#hubway', {prov.model.PROV_LABEL:'Hubway', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubway, this_script)
        doc.wasGeneratedBy(hubway, get_hubway, endTime)
        
        for i in range(37):
            command = "doc.wasDerivedFrom(hubway, resource"+str(i)+", get_hubway, get_hubway, get_hubway)"
            exec(command)
        
        repo.logout()          
        return doc


# retrieve_hubway.execute(log=True)
doc = retrieve_hubway.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
