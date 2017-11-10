import dml
import prov.model
import datetime
import uuid


class selectImpBuilds(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.property_assessment']
    writes = ['bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds']

    @staticmethod
    def execute(trial=False):
        '''Select the addresses of important buildings from the Property Assessment data set'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo
        collection = db['bkin18_cjoe_klovett_sbrz.property_assessment']

        ## Build a list out of valid residential property IDs
        addr_list = []
        '''117: Day Care Use, 118: Elederly Home, 305: Hospital, 307: Veterinary Hospital, 309: 
        Medical Clinic, 324: Supermarket, 333: Gas Station, 336: Parking Garage, 341: Bank Building, 
        352: Daycare Use, 378: School, 425: Gas Manufacture Plant, 426: Gas Pipeline Row, 904: Priv School/College, 
        962: Parking Lot, 974: Fire Station, 975: Police Station, 976: School, 977: College, 979: Hospital'''
        property_lookup = ['117', '118', '305', '307', '309', '324', '333', '336', '341', '352', 
        '378', '425', '426', '904', '962', '974', '975', '976', '977', '979']

        modifiedDictionary = []

        #Obtains important property types and their latitude and longitudes.
        for building in repo['bkin18_cjoe_klovett_sbrz.property_assessment'].find():

            if (building['PTYPE'] in property_lookup):

                modifiedPiece = {'PTYPE': building['PTYPE'], 'LONGITUDE': building['LONGITUDE'], 'LATITUDE': building['LATITUDE']}
                modifiedDictionary.append(modifiedPiece)

        repo.dropCollection('bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds')
        repo.createCollection('bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds')
        repo['bkin18_cjoe_klovett_sbrz.property_assessment_impBuilds'].insert_many(modifiedDictionary)
  
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
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#DataSet')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#selectImpBuilds',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        #This line might need some modification - Keith
        resource = doc.entity('bdp:062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label': 'Modified Property Data', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        property_address_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#property_assessment',
            {'prov:label': 'property_assessment', prov.model.PROV_TYPE: 'ont:DataSet'})
        impBuilds_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#property_assessment_impBuilds',
                                {'prov:label': 'property_assessment_impBuilds', prov.model.PROV_TYPE: 'ont:DataSet'})
        select_impBuilds_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(select_impBuilds_data, this_script)
        #I don't think this is actually of type "retrieval," I'm just not sure what the actual name for it is atm. - Keith
        doc.usage(select_impBuilds_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=address+data&$select=description'
                  }
                  )

        doc.wasAttributedTo(impBuilds_db, this_script)
        doc.wasGeneratedBy(impBuilds_db, select_impBuilds_data, endTime)
        doc.wasDerivedFrom(property_address_db, resource, impBuilds_db)

        repo.logout()

        return doc
