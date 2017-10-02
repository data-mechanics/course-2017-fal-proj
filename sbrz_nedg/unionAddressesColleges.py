import dml
import prov.model
import datetime
import uuid


class unionAddressesColleges(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = ['sbrz_nedg.property_assessment_addresses', 'sbrz_nedg.college_university_addresses']
    writes = ['sbrz_nedg.union_addresses_colleges']
    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston property assessment data set.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        db = client.repo
        college_address_collection = db['sbrz_nedg.college_university_addresses']
        property_assessment_address_collection = db['sbrz_nedg.property_assessment_addresses']

        college_addresses = college_address_collection.find()
        property_addresses = property_assessment_address_collection.find()

        x = []
        count = 0
        for property_address in property_addresses:
            property_address['num_schools'] = 0
            college_addresses = college_address_collection.find()
            for college_address in college_addresses:
                if property_address['ZIPCODE'] == college_address['properties']['Zipcode']:
                    property_address['num_schools'] += 1
            x.append(property_address)

        repo.dropCollection('sbrz_nedg.union_addresses_colleges')
        repo.createCollection('sbrz_nedg.union_addresses_colleges')
        repo['sbrz_nedg.union_addresses_colleges'].insert_many(x)
        repo['sbrz_nedg.union_addresses_colleges'].metadata({'complete': True})

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
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:sbrz_nedg#example',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_found = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        lost = doc.entity('dat:sbrz_nedg#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:sbrz_nedg#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()

        return doc

unionAddressesColleges.execute()