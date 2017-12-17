import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import math

class neighborhoodScores(dml.Algorithm):
    contributor = 'carole07_echanglc_wongi'
    reads = ['carole07_echanglc_wongi.hospitals', 'carole07_echanglc_wongi.streetlights','carole07_echanglc_wongi.schools', 'carole07_echanglc_wongi.camSchools', 'carole07_echanglc_wongi.polices']
    writes = ['carole07_echanglc_wongi.hospitals_coord', 'carole07_echanglc_wongi.schools_coord', 'carole07_echanglc_wongi.streetlights_coord', 'carole07_echanglc_wongi.neighborhood_scores', 'carole07_echanglc_wongi.polices_coord']

    @staticmethod
    def execute(trial = True):

        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
############################################################################################################
        Hospitals_coord = []
        Hospitals_dict = []
        CurrentHosp = repo['carole07_echanglc_wongi.hospitals'].find()
        for entry in CurrentHosp:
            name = entry['NAME']
            coord = entry['Location'].splitlines()[2]
            pair = coord.split(",")
            coord = [float(pair[0][1:]),float(pair[1][1:-1])]
            Hospitals_coord.append([name,coord])
            Hospitals_dict.append({'hospitalName':name,'coord':coord})
        repo.dropPermanent("hospitals_coord")
        repo.createPermanent("hospitals_coord")
        repo['carole07_echanglc_wongi.hospitals_coord'].insert_many(Hospitals_dict)
############################################################################################################
        Schools_coord = []
        Schools_dict = []
        CurrentSchool = repo['carole07_echanglc_wongi.schools'].find()
        CurrentCamSchool = repo['carole07_echanglc_wongi.camSchools'].find()                      
        for entry in CurrentSchool:
            coord = entry['fields']['geo_point_2d']
            name = entry['fields']['sch_name']
            Schools_coord.append([name,coord])
            Schools_dict.append({'schoolName':name,'coord':coord})
        tempcoords = []
        tempnames = [
            "Haggarty School",
            "John M. Tobin School",
            "Andrew Peabody School",
            "Graham & Parks School",
            "Maria L. Baldwin School",
            "Putnam Avenue Upper School",
            "Morse Elementary School",
            "Dr. Martin Luther King Jr. School",
            "Cambridge Rindge & Latin School",
            "CRLS 9th Grade Campus",
            "High School Extension Program",
            "Amigos School",
            "King Open School",
            "Cambridgeport School",
            "Fletcher-Maynard Elementary",
            "Kennedy/Longfellow School",
            "Cambridge Street Upper School"
            ]
        for entry in CurrentCamSchool:
            for entry2 in entry["meta"]["view"]["columns"]:
                if entry2["id"] == 232084408:
                    for entry3 in entry2["cachedContents"]["top"]:
                        tempcoords += entry3["item"]["coordinates"]
        for i in range(len(tempnames)):
            coord = [tempcoords[i*2-1], tempcoords[i*2]]
            name = tempnames[i]
            Schools_coord.append([name,coord])
            Schools_dict.append({'schoolName':name,'coord':coord})
        repo.dropPermanent("schools_coord")
        repo.createPermanent("schools_coord")
        repo['carole07_echanglc_wongi.schools_coord'].insert_many(Schools_dict)
############################################################################################################
        Streetlights_coord = []
        Streetlights_dict = []
        LightTypes = repo['carole07_echanglc_wongi.streetlights'].find()
        for entry in LightTypes:
            coord = [entry['Lat'],entry['Long']]
            name = entry["TYPE"]
            Streetlights_coord.append([name,coord])
            Streetlights_dict.append({'streetlightName':name,'coord':coord})
        repo.dropPermanent("streetlights_coord")
        repo.createPermanent("streetlights_coord")
        repo['carole07_echanglc_wongi.streetlights_coord'].insert_many(Streetlights_dict)
############################################################################################################
        Polices_coord = []
        Polices_dict = []
        CurrentPoliceDept = repo['carole07_echanglc_wongi.polices'].find()
        hardcoords = [
            [42.286760, -71.148411],
            [42.256476, -71.124279],
            [42.309700, -71.104600],
            [42.339629, -71.069161],
            [42.349300, -71.150600],
            [42.341200, -71.054900],
            [42.298068, -71.059141],
            [42.284800, -71.091600],
            [42.328494, -71.085717],
            [42.371200, -71.038700]]
        count = 0
        for entry in CurrentPoliceDept:
            for entry2 in entry["data"]["fields"]:
                if entry2["name"] == "NAME":
                    for entry3 in entry2["statistics"]["values"]:
                        name = entry3["value"]
                        coord = hardcoords[count]
                        Polices_coord.append([name,coord])
                        Polices_dict.append({'policeDeptName':name,'coord':coord})
                        count+=1
        repo.dropPermanent("polices_coord")
        repo.createPermanent("polices_coord")
        repo["carole07_echanglc_wongi.polices_coord"].insert_many(Polices_dict)
############################################################################################################
        neighborhoods = [
        ['Allston', [42.3539, -71.1337]],
        ['Back Bay', [42.3503, -71.0810]],
        ['Bay Village', [42.3490, -71.0698]],
        ['Beacon Hill', [42.3588, -71.0707]],
        ['Brighton', [42.3464, -71.1627]],
        ['Charlestown', [42.3782, -71.0602]],
        ['Chinatown', [42.3501, -71.0624]],
        ['Dorchester', [42.3016, -71.0676]],
        ['Downtown Crossing', [42.3555, -71.0594]],
        ['East Boston', [42.3702, -71.0389]],
        ['Fenway', [42.3429, -71.1003]],
        ['Hyde Park', [42.2565, -71.1241]],
        ['Jamaica Plain', [42.3097, -71.0476]],
        ['Mattapan', [42.2771, -71.0914]],
        ['Mission Hill', [42.3296, -71.1062]],
        ['North End', [42.3647, -71.0542]],
        ['Roslindale', [42.2832, -71.1270]],
        ['Roxbury', [42.3152, -71.0914]],
        ['South Boston', [42.3381, -71.0476]],
        ['South End', [42.3388, -71.0765]],
        ['West End', [42.3644, -71.0661]],
        ['West Roxbury', [42.2798, -71.1627]]
        ]
############################################################################################################
        def getDistance(lat1,lon1,lat2,lon2):
          R = 6371
          dLat = deg2rad(lat2-lat1)
          dLon = deg2rad(lon2-lon1)
          a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
          c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
          d = R * c
          return d


        def deg2rad(deg):
          return deg * (math.pi/180)

        # Optimizing distance by min (optimization problem)
        def minHospital(category):
            if (trial):
                category = category[:5]

            distancesPerCity = [0] * len(neighborhoods)


            for i in range(len(neighborhoods)):
                minDistance = 1000000
                for t in range(len(category)):
                    currentDistance = getDistance( neighborhoods[i][1][0] , neighborhoods[i][1][1] , float(category[t][1][0]), float(category[t][1][1]) )
                    if currentDistance < minDistance:
                        minDistance = currentDistance
                        distancesPerCity[i] = [neighborhoods[i],category[t],minDistance]

            return distancesPerCity

        # Constraint using threshold
        def countCategory(category):
            if (trial):
                category = category[:5]

            countPerCity = [0] * len(neighborhoods)
            threshold = 3

            for i in range(len(neighborhoods)):
                count = 0

                for t in range(len(category)):
                    currentDistance = getDistance( neighborhoods[i][1][0] , neighborhoods[i][1][1] , float(category[t][1][0]), float(category[t][1][1]) )
                    if currentDistance < threshold: #category is within 3km of the neighborhood
                        count += 1
                        countPerCity[i] = [neighborhoods[i],count] #update count for respective neighborhood
                if count == 0:
                    countPerCity[i] = [neighborhoods[i],count]

            return countPerCity

        def propCalc(data):
            if (trial):
                data = data[:5]

            countPerCity = [0] * len(neighborhoods)
            threshold = 3

            for i in range(len(neighborhoods)):
                calc = 0
                count = 0

                for t in range(len(data)):
                    currentDistance = getDistance(neighborhoods[i][1][0], neighborhoods[i][1][1], float(data[t][1][0]), float(data[t][1][1]))
                    if currentDistance < threshold: # the property is within 3km radius
                        calc += int(data[t][-1])
                        count += 1
                        countPerCity[i] = [neighborhoods[i], calc]
                if calc == 0:
                    countPerCity[i] = [neighborhoods[i], calc]
                countPerCity[i] = [neighborhoods[i], calc//count]
                # divide total value of all nearby residence properties by the number of nearby residence properties
            return countPerCity

        #count of each category per neighborhood
        streetlights_Count = countCategory(Streetlights_coord)
        hospital_Count = minHospital(Hospitals_coord)
        #print(hospital_Count)
        school_Count = countCategory(Schools_coord)
        polices_Count = minHospital(Polices_coord)


        result = [[x for x in range(2)] for y in range(len(neighborhoods))]

        a = []
        # Scoring algorithm
        for i in range(len(neighborhoods)):
            result[i][0] = neighborhoods[i][0]

            # Calculate score
            result[i][1] = hospital_Count[i][-1] * 0.25
            result[i][1] += school_Count[i][-1] * 0.25
            result[i][1] += polices_Count[i][-1] * 0.25
            result[i][1] += streetlights_Count[i][-1] * 0.25
            result[i][1] /= 4
            a.append({'neighborhood' : result[i][0], 'hospital_count': hospital_Count[i][-1], 'school_count': school_Count[i][-1], 'policeDept_count': polices_Count[i][-1], 'streetlights_count': streetlights_Count[i][-1], 'score': (result[i][1])})

        print(a)
        repo.dropPermanent("neighborhood_scores")
        repo.createPermanent("neighborhood_scores")
        repo['carole07_echanglc_wongi.neighborhood_scores'].insert_many(a)
        endTime = datetime.datetime.now()
        repo.logout()
        return {"start":startTime, "end":endTime}



    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        """
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carole07_echanglc_wongi', 'carole07_echanglc_wongi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:carole07_echanglc_wongi#getneighborhoodScores', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        hospital_resource = doc.entity('dat:carole07_echanglc_wongi#hospitals', {'prov:label':' Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        school_resource = doc.entity('dat:carole07_echanglc_wongi#schools', {'prov:label':' Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        camSchool_resource = doc.entity('dat:carole07_echanglc_wongi#camSchools', {'prov:label':'Cambridge Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        police_resource = doc.entity('dat:carole07_echanglc_wongi#polices', {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        streetlights_resource = doc.entity('dat:carole07_echanglc_wongi#streetlights', {'prov:label':'Streetlights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhoodScores = doc.entity('dat:carole07_echanglc_wongi#neighborhood_scores', {prov.model.PROV_LABEL: 'Scores of each Boston neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        get_neighborhoodScores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_neighborhoodScores, this_script)
        
        doc.usage(get_neighborhoodScores, school_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_neighborhoodScores, hospital_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_neighborhoodScores, streetlights_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_neighborhoodScores, camSchool_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_neighborhoodScores, police_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        neighborhoodScores = doc.entity('dat:carole07_echanglc_wongi#neighborhoodScores', {prov.model.PROV_LABEL:' Complete Dev Scores', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(neighborhoodScores, this_script)
        doc.wasGeneratedBy(neighborhoodScores, get_neighborhoodScores, endTime)

        doc.wasDerivedFrom(neighborhoodScores, camSchool_resource, get_neighborhoodScores, get_neighborhoodScores, get_neighborhoodScores)
        doc.wasDerivedFrom(get_neighborhoodScores, school_resource, get_neighborhoodScores, get_neighborhoodScores, get_neighborhoodScores)
        doc.wasDerivedFrom(get_neighborhoodScores, hospital_resource, get_neighborhoodScores, get_neighborhoodScores, get_neighborhoodScores)
        doc.wasDerivedFrom(get_neighborhoodScores, streetlights_resource, get_neighborhoodScores, get_neighborhoodScores, get_neighborhoodScores)
        doc.wasDerivedFrom(get_neighborhoodScores, police_resource, get_neighborhoodScores, get_neighborhoodScores, get_neighborhoodScores)
        
        repo.logout()
        return doc
