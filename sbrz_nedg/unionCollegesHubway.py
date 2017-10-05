import dml
import geopy.distance
from geopy import Point
import prov.model
import datetime
import uuid


class unionCollegesHubway(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = ['sbrz_nedg.college_coords', 'sbrz_nedg.hubway']
    writes = ['sbrz_nedg.union_colleges_hubway']
    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        db = client.repo
        college_coords_collection = db['sbrz_nedg.college_coords']
        hubway_coords_collection = db['sbrz_nedg.hubway']

        college_coords = college_coords_collection.find()
        hubway_coords = hubway_coords_collection.find()

        x = []
        count = 0
        for college in college_coords:
            college['hubway_station_count'] = 0
            hubway_coords = hubway_coords_collection.find()
            college_coordinates = Point(float(college['properties']['Latitude']), float(college['properties']['Longitude']))
            for station in hubway_coords:
                station_coords = Point(float(station['geometry']['coordinates'][1]), float(station['geometry']['coordinates'][0]))
                distance = geopy.distance.distance(station_coords, college_coordinates).miles
                if distance <= 0.5:
                   college['hubway_station_count'] += 1
            x.append(college)

        repo.dropCollection('sbrz_nedg.union_colleges_hubway')
        repo.createCollection('sbrz_nedg.union_colleges_hubway')
        repo['sbrz_nedg.union_colleges_hubway'].insert_many(x)
        repo['sbrz_nedg.union_colleges_hubway'].metadata({'complete': True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#DataSet')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:sbrz_nedg#unionCollegesHubway', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        college_coords_db = doc.entity('dat:sbrz_nedg#college_coords', {'prov:label': 'college_coords', prov.model.PROV_TYPE: 'ont:DataSet'})
        hubway_coords_db = doc.entity('dat:sbrz_nedg#hubway',
                                        {'prov:label': 'hubway', prov.model.PROV_TYPE: 'ont:DataSet'})
        union_db = doc.entity('dat:sbrz_nedg#union_colleges_hubway',
                                        {'prov:label': 'union_colleges_hubway', prov.model.PROV_TYPE: 'ont:DataSet'})
        union_addresses_colleges = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(college_coords_db, hubway_coords_db, union_db, startTime, None)

        doc.wasAttributedTo(this_script, this_script, this_script)
        doc.wasGeneratedBy(union_addresses_colleges)
        doc.wasDerivedFrom(college_coords_db, hubway_coords_db, union_db)

        repo.logout()

        return doc
