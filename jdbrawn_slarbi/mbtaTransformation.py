import dml
import prov.model
import datetime
import uuid
import gpxpy.geo

class mbtaTransformation(dml.Algorithm):

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
    reads = ['jdbrawn_slarbi.colleges', 'jdbrawn_slarbi.mbta']
    writes = ['jdbrawn_slarbi.mbtaAnalysis']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_slarbi', 'jdbrawn_slarbi')

        colleges = repo['jdbrawn_slarbi.colleges']
        mbta = repo['jdbrawn_slarbi.mbta']

        collegeLocations = []
        numStudents = []
        mbtaLocations = []

        # clean college data to just include name and lat/long and also make another list with the number of students
        for entry in colleges.find():
            if 'Latitude' in entry and entry['Latitude'] != '0':
                collegeLocations.append((entry['Name'], float(entry['Latitude']), float(entry['Longitude']), int(entry['NumStudent'])))
                numStudents.append((entry['Name'], int(entry['NumStudent'])))

        # clean mbta data to just include id number and lat/long
        for entry in mbta.find():
            if 'geometry' in entry:
                mbtaLocations.append((entry['properties']['STOP_ID'], entry['geometry']['coordinates']))

        # find total number of stops within a mile of each school
        school_and_stops = []
        for uni in collegeLocations:
            for stop in mbtaLocations:
                if gpxpy.geo.haversine_distance(uni[1], uni[2], stop[1][1], stop[1][0]) < 1610:
                    school_and_stops.append((uni[0], 1))
        school_and_stops = mbtaTransformation.aggregate(school_and_stops, sum)

        # combine the previous two to get (school, number of crimes, number of crashes)
        product_select_project = mbtaTransformation.project(mbtaTransformation.select(mbtaTransformation.product(school_and_stops, numStudents), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        # format it for MongoDB
        transformed_data = []
        for entry in product_select_project:
            transformed_data.append({'Name': entry[0], 'Number of MBTA stops': entry[1], 'Number of Students': entry[2]})

        repo.dropCollection('mbtaAnalysis')
        repo.createCollection('mbtaAnalysis')
        repo['jdbrawn_slarbi.mbtaAnalysis'].insert_many(transformed_data)

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
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_slarbi/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jdbrawn_slarbi#mbtaTransformation', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_colleges = doc.entity('dat:jdbrawn_slarbi#colleges', {'prov:label': 'Boston Universities and Colleges', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_mbta = doc.entity('dat:jdbrawn_slarbi#mbta', {'prov:label': 'MBTA Bus Stops', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_mbtaAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_mbtaAnalysis, this_script)

        doc.usage(get_mbtaAnalysis, resource_colleges, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_mbtaAnalysis, resource_mbta, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        mbta = doc.entity('dat:jdbrawn_slarbi#mbtaAnalysis', {prov.model.PROV_LABEL: 'MBTA Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_mbtaAnalysis, endTime)
        doc.wasDerivedFrom(mbta, resource_colleges, get_mbtaAnalysis, get_mbtaAnalysis, get_mbtaAnalysis)
        doc.wasDerivedFrom(mbta, resource_mbta, get_mbtaAnalysis, get_mbtaAnalysis, get_mbtaAnalysis)

        repo.logout()

        return doc
