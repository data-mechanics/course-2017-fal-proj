import dml
import prov.model
import datetime
import uuid
import sys

class aggregateRoutes(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = ['bkin18_cjoe_klovett_sbrz.roads_inventory']
    writes = ['bkin18_cjoe_klovett_sbrz.routes_agg']

    @staticmethod
    def execute(trial=False):
        '''Aggregate based off of routes in the roads inventory data set'''
        startTime = datetime.datetime.now()

        print("Aggregating routes...          \n", end='\r')
        sys.stdout.write("\033[F") # Cursor up one line

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
        db = client.repo
        collection = db['bkin18_cjoe_klovett_sbrz.roads_inventory']

        ## Create dictionary to store route data
        route_dict = {}
        roads = collection.find()

        ## Format of list: [Street_Name, From_Street_Name, To_Street_Name]
        for road in roads:
            try:
                ## To remove duplicates
                if [road['St_Name'], road['Fm_St_Name'], road['To_St_Name']] not in route_dict[road['Route_ID']]:
                    route_dict[road['Route_ID']].append([ road['St_Name'], road['Fm_St_Name'], road['To_St_Name'] ])
            except Exception as e:
                route_dict[road['Route_ID']] = [[ road['St_Name'], road['Fm_St_Name'], road['To_St_Name'] ]]
 
        repo.dropCollection('bkin18_cjoe_klovett_sbrz.routes_agg')
        repo.createCollection('bkin18_cjoe_klovett_sbrz.routes_agg')
        repo['bkin18_cjoe_klovett_sbrz.routes_agg'].insert(route_dict)
        repo['bkin18_cjoe_klovett_sbrz.routes_agg'].metadata({'complete': True})
        # repo.dropCollection('bkin18_cjoe_klovett_sbrz.property_assessment')

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

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#aggregateRoutes',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        #This line might need some modification - Keith
        resource = doc.entity('bdp:062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label': 'Aggregated Route ID', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        property_address_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#route_agg',
            {'prov:label': 'route_agg', prov.model.PROV_TYPE: 'ont:DataSet'})
        address_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#route_agg',
                                {'prov:label': 'route_agg', prov.model.PROV_TYPE: 'ont:DataSet'})
        select_address_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(select_address_data, this_script)
        #I don't think this is actually of type "retrieval," I'm just not sure what the actual name for it is atm. - Keith
        doc.usage(select_address_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'db.bkin18_cjoe_klovett_sbrz.roads_inventory.find()'
                  }
                  )

        doc.wasAttributedTo(address_db, this_script)
        doc.wasGeneratedBy(address_db, select_address_data, endTime)
        doc.wasDerivedFrom(property_address_db, resource, address_db)

        repo.logout()

        return doc
