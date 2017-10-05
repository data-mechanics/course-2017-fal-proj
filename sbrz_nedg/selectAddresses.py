import dml
import prov.model
import datetime
import uuid


class selectAddresses(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = ['sbrz_nedg.property_assessment']
    writes = ['sbrz_nedg.property_assessment_addresses']

    @staticmethod
    def execute(trial=False):
        '''Select all of the addresses from the Property Assessment data set'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        db = client.repo
        collection = db['sbrz_nedg.property_assessment']
        x = []
        addresses = collection.find({}, {'MAIL_ADDRESS': 1, 'MAIL CS': 1, 'ZIPCODE': 1})
        for address in addresses:
            address['ZIPCODE'] = address['ZIPCODE'].strip('_') # strip off trailing underline
            if 'PO BOX' not in address['MAIL_ADDRESS']:  # filter out PO Boxes
                x.append(address)

        repo.dropCollection('sbrz_nedg.property_assessment_addresses')
        repo.createCollection('sbrz_nedg.property_assessment_addresses')
        repo['sbrz_nedg.property_assessment_addresses'].insert_many(x)
        repo['sbrz_nedg.property_assessment_addresses'].metadata({'complete': True})
        repo.dropCollection('sbrz_nedg.property_assessment')

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

        this_script = doc.agent('alg:sbrz_nedg#selectAddressesColleges',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        property_address_db = doc.entity('dat:sbrz_nedg#property_assessment',
            {'prov:label': 'property_assessment', prov.model.PROV_TYPE: 'ont:DataSet'})
        address_db = doc.entity('dat:sbrz_nedg#property_assessment_addresses',
                                {'prov:label': 'property_assessment_addresses', prov.model.PROV_TYPE: 'ont:DataSet'})
        select_address_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(property_address_db, address_db, startTime)

        doc.wasAttributedTo(this_script, this_script)
        doc.wasGeneratedBy(select_address_data)
        doc.wasDerivedFrom(property_address_db, address_db)

        repo.logout()

        return doc
