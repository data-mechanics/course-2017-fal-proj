import dml
import prov.model
import datetime
import uuid
import gpxpy.geo

class safetyTranformation(dml.Algorithm):

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    contributor = 'jdbrawn_slarbi'
    reads = ['jdbrawn_slarbi.colleges', 'jdbrawn_slarbi.crime', 'jdbrawn_slarbi.crash']
    writes = ['jdbrawn_slarbi.safetyAnalysis']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        colleges = repo['jdbrawn_slarbi.colleges']
        crime = repo['jdbrawn_slarbi.crime']
        crash = repo['jdbrawn_slarbi.crash']

        collegeLocations = []
        crimeLocations = []
        crashLocations = []

        # clean college data to just include name and lat/long
        for entry in colleges.find():
            if 'Latitude' in entry and entry['Latitude'] != '0':
                collegeLocations.append((entry['Name'], float(entry['Latitude']), float(entry['Longitude'])))

        # clean crime data to just include id number and lat/long
        for entry in crime.find():
            if 'Lat' in entry and type(entry['Lat']) == str:
                crimeLocations.append((entry['_id'], float(entry['Lat']), float(entry['Long'])))

        # clean crash data to just include id number and lat/long
        for entry in crash.find():
            if 'Latitude' in entry:
                crashLocations.append((entry['Crash Number'], float(entry['Latitude']), float(entry['Longitude'])))

        # find all crimes within a mile of each school
        school_and_crime = []
        for uni in collegeLocations:
            for act in crimeLocations:
                if gpxpy.geo.haversine_distance(uni[1], uni[2], act[1], act[2]) < 1610:
                    school_and_crime.append((uni[0], 1))
        school_and_crime = safetyTranformation.aggregate(school_and_crime, sum)

        # find all crashes within a mile of each school
        school_and_crash = []
        for uni in collegeLocations:
            for act in crashLocations:
                if gpxpy.geo.haversine_distance(uni[1], uni[2], act[1], act[2]) < 1610:
                    school_and_crash.append((uni[0], 1))
        school_and_crash = safetyTranformation.aggregate(school_and_crash, sum)

        # combine the previous two to get (school, number of crimes, number of crashes)
        product_select_project = safetyTranformation.project(safetyTranformation.select(safetyTranformation.product(school_and_crime,school_and_crash), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        # format it for MongoDB
        transformed_data = []
        for entry in product_select_project:
            transformed_data.append({'Name': entry[0], 'Number of Crimes': entry[1], 'Number of Crashes': entry[2]})

        repo.dropCollection('safetyAnalysis')
        repo.createCollection('safetyAnalysis')
        repo['jdbrawn_slarbi.safetyAnalysis'].insert_many(transformed_data)

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
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('car', 'http://datamechanics.io/data/jdbrawn_slarbi/')

        this_script = doc.agent('alg:jdbrawn_slarbi#safetyTransformation', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_colleges = doc.entity('dat:jdbrawn_slarbi#colleges', {'prov:label': 'Boston Universities and Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_crime = doc.entity('dat:jdbrawn_slarbi#crime', {'prov:label': 'Boston Crime', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_crashes = doc.entity('dat:jdbrawn_slarbi#crash', {'prov:label': 'Boston Crashes', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_safetyAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_safetyAnalysis, this_script)

        doc.usage(get_safetyAnalysis, resource_colleges, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_safetyAnalysis, resource_crime, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_safetyAnalysis, resource_crashes, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        safety = doc.entity('dat:jdbrawn_slarbi#safetyAnalysis', {prov.model.PROV_LABEL: 'Safety Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(safety, this_script)
        doc.wasGeneratedBy(safety, get_safetyAnalysis, endTime)
        doc.wasDerivedFrom(safety, resource_colleges, get_safetyAnalysis, get_safetyAnalysis, get_safetyAnalysis)
        doc.wasDerivedFrom(safety, resource_crime, get_safetyAnalysis, get_safetyAnalysis, get_safetyAnalysis)
        doc.wasDerivedFrom(safety, resource_crashes, get_safetyAnalysis, get_safetyAnalysis, get_safetyAnalysis)

        repo.logout()

        return doc
