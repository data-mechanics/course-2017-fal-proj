
import dml
import prov.model
import datetime
import uuid
import gpxpy.geo

class policeAnalysis(dml.Algorithm):
    
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.safetyScore', 'jdbrawn_jliang24_slarbi_tpotye.police', 'jdbrawn_jliang24_slarbi_tpotye.colleges']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.policeAnalysis']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        safetyScore = repo['jdbrawn_jliang24_slarbi_tpotye.safetyScore'] 
        police = repo['jdbrawn_jliang24_slarbi_tpotye.police']
        colleges= repo['jdbrawn_jliang24_slarbi_tpotye.colleges']


        collegeLocations = []
        policeLocations = []

        # clean college data to just include name and lat/long
        for entry in colleges.find():
            if 'Latitude' in entry and entry['Latitude'] != '0':
                collegeLocations.append((entry['Name'], float(entry['Latitude']), float(entry['Longitude'])))

        # clean property data to just include police station location
        for entry in police.find():
            policeLocations.append((float(entry['Y']), float(entry['X'])))


        # find all police stations within a mile of each school
        school_and_police = []
        for uni in collegeLocations:
            school_and_police.append((uni[0], 0))
            for act in policeLocations:
                if gpxpy.geo.haversine_distance(uni[1], uni[2], act[0], act[1]) < 1610:
                    school_and_police.append((uni[0], 1))
        school_and_police = policeAnalysis.aggregate(school_and_police, sum)

        # format it for MongoDB
        schoolPolice_data = []
        for entry in school_and_police:
            safetyscore= (safetyScore.find_one({'Name':entry[0]})['Safety Score'])
            numPolice= entry[1]
            schoolPolice_data.append({'Safety Score': safetyscore, 'Num Police': numPolice})

        repo.dropCollection('policeAnalysis')
        repo.createCollection('policeAnalysis')
        repo['jdbrawn_jliang24_slarbi_tpotye.policeAnalysis'].insert_many(schoolPolice_data)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """

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

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#policeAnalysis', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_colleges = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#colleges', {'prov:label': 'Boston Universities and Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_police= doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#police' , {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_safetyScore = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safetyScore', {'prov:label': 'Safety Score', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_policeAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_policeAnalysis, this_script)

        doc.usage(get_policeAnalysis, resource_colleges, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_policeAnalysis, resource_police, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_policeAnalysis, resource_safetyScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        policeVal = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#policeAnalysis', {prov.model.PROV_LABEL: 'Police Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(policeVal, this_script)
        doc.wasGeneratedBy(policeVal, get_policeAnalysis, endTime)
        doc.wasDerivedFrom(policeVal, resource_colleges, get_policeAnalysis, get_policeAnalysis, get_policeAnalysis)
        doc.wasDerivedFrom(policeVal, resource_police, get_policeAnalysis, get_policeAnalysis, get_policeAnalysis)
        doc.wasDerivedFrom(policeVal, resource_safetyScore, get_policeAnalysis, get_policeAnalysis, get_policeAnalysis)

        repo.logout()

        return doc
