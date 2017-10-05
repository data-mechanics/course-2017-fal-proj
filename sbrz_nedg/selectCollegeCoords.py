import dml
import prov.model
import datetime
import uuid


class selectCollegeCoords(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = ['sbrz_nedg.college_university']
    writes = ['sbrz_nedg.college_coords']

    @staticmethod
    def execute(trial=False):
        '''Select all of the addresses from the College/Universities data set'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        db = client.repo
        collection = db['sbrz_nedg.college_university']
        x = []
        colleges = collection.find({}, {'properties.Name': 1, 'properties.Latitude': 1, 'properties.Longitude': 1})
        for college in colleges:
            x.append(college)

        repo.dropCollection('sbrz_nedg.college_coords')
        repo.createCollection('sbrz_nedg.college_coords')
        repo['sbrz_nedg.college_coords'].insert_many(x)
        repo['sbrz_nedg.college_coords'].metadata({'complete': True})

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
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#DataSet')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:sbrz_nedg#selectCollegeCoords',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        college_db = doc.entity('dat:sbrz_nedg#college_coords',
                                {prov.model.PROV_LABEL: 'college_coords', prov.model.PROV_TYPE: 'ont:DataSet'})
        college_coord_db = doc.entity('dat:sbrz_nedg#college_db',
                                      {prov.model.PROV_LABEL: 'college_db', prov.model.PROV_TYPE: 'ont:DataSet'})
        select_college_coordinates = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(select_college_coordinates, this_script)
        doc.usage(college_coord_db, college_db, startTime, None)

        doc.wasAttributedTo(this_script, this_script)
        doc.wasGeneratedBy(select_college_coordinates)
        doc.wasDerivedFrom(college_coord_db, college_db)

        repo.logout()

        return doc
