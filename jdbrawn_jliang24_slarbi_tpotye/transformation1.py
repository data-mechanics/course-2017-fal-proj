import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import gpxpy.geo
from ast import literal_eval as make_tuple


class transformation1(dml.Algorithm):
    def union(R, S):
        return R + S
    def intersect(R, S):
        return [t for t in R if t in S]
    def product(R, S):
        return [(t,u) for t in R for u in S]
    def select(R, s):
        return [t for t in R if s(t)]
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    def removeDuplicates(seq):
        #helper function from previous semester
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x)) and x != " "]


    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.entertain',
              'jdbrawn_jliang24_slarbi_tpotye.food',
             'jdbrawn_jliang24_slarbi_tpotye.colleges']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.socialAnalysis']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        entertainmentLoc = repo['jdbrawn_jliang24_slarbi_tpotye.entertain']
        foodLoc = repo['jdbrawn_jliang24_slarbi_tpotye.food']
        colleges = repo['jdbrawn_jliang24_slarbi_tpotye.colleges']

        #begin transformation

        collegeLocations = []
        foodLocations = []
        entertainmentLocations = []

        # clean college data to just include name and lat/long
        for entry in colleges.find():
            if 'Latitude' in entry and entry['Latitude'] != '0':
                collegeLocations.append((entry['Name'], float(entry['Latitude']), float(entry['Longitude'])))

        # clean food data to just include id number and lat/long
        for entry in foodLoc.find():
            if 'location' in entry:
                foodLocations.append((1, float(entry['location']['coordinates'][1]), float(entry['location']['coordinates'][0])))

        # clean entertainment data to just include id number and lat/long
        for entry in entertainmentLoc.find():
            if 'location' in entry and entry['location'] != "NULL":
                lat_long = make_tuple(entry['location'])
                entertainmentLocations.append(
                    (1, float(lat_long[0]), float(lat_long[1])))

        # find all food within a mile of each school
        school_and_food = []
        for uni in collegeLocations:
            school_and_food.append((uni[0], 0))
            for act in foodLocations:
                if gpxpy.geo.haversine_distance(uni[1], uni[2], act[1], act[2]) < 1610:
                    school_and_food.append((uni[0], 1))
        school_and_food = transformation1.aggregate(school_and_food, sum)

        # find all entertainment within a mile of each school
        school_and_entertainment = []
        for uni in collegeLocations:
            school_and_entertainment.append((uni[0], 0))
            for act in entertainmentLocations:
                if gpxpy.geo.haversine_distance(uni[1], uni[2], act[1], act[2]) < 1610:
                    school_and_entertainment.append((uni[0], 1))
        school_and_entertainment = transformation1.aggregate(school_and_entertainment, sum)

        # combine the previous two to get (school, number of crimes, number of crashes)
        product_select_project = transformation1.project(
            transformation1.select(transformation1.product(school_and_food, school_and_entertainment),
                                       lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        # format it for MongoDB
        transformed_data = []
        for entry in product_select_project:
            transformed_data.append({'Name': entry[0], 'Number of Food': entry[1], 'Number of Entertainment': entry[2]})

        repo.dropCollection('socialAnalysis')
        repo.createCollection('socialAnalysis')
        repo['jdbrawn_jliang24_slarbi_tpotye.socialAnalysis'].insert_many(transformed_data)

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
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_jliang24_slarbi_tpotye/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')


        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_entertain = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#entertain', {'prov:label':'Entertainment Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_food = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#food', {'prov:label': 'Food Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_socialAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_socialAnalysis, this_script)

        doc.usage(get_socialAnalysis, resource_entertain, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_socialAnalysis, resource_food, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        social = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#socialAnalysis', {prov.model.PROV_LABEL: 'Social Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(social, this_script)
        doc.wasGeneratedBy(social, get_socialAnalysis, endTime)
        doc.wasDerivedFrom(social, resource_entertain, get_socialAnalysis, get_socialAnalysis, get_socialAnalysis)
        doc.wasDerivedFrom(social, resource_food, get_socialAnalysis, get_socialAnalysis, get_socialAnalysis)
        repo.logout()

        return doc



