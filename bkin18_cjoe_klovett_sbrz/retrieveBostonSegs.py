import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid
import sys


class retrieveBostonSegs(dml.Algorithm):
    contributor = 'bkin18_cjoe_klovett_sbrz'
    reads = []
    writes = ['bkin18_cjoe_klovett_sbrz.road_segs']

    @staticmethod
    def execute(trial=False):
        '''Retrieve Boston property assessment data set.'''

        print("Retrieving roads inventory...         \n", end='\r')
        sys.stdout.write("\033[F") # Cursor up one line

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')

        # Setting up our API call
        SAMPLE_START = 2816
        TRIAL_NUM = 1000
        
        # Checks to see whether we are doing a trial execute or not - used a range because our starting data has a lot of empty points
        #REMEMBER TO INCLUDE IN TRIAL - Keith
        '''
        if trial:
            url = "http://gis.massdot.state.ma.us/arcgis/rest/services/Roads/RoadInventory/MapServer/0/query?where=OBJECTID%20%3E%3D%20" + str(SAMPLE_START) + "%20AND%20OBJECTID%20%3C%3D%20" + str(SAMPLE_START + TRIAL_NUM) + "&outFields=*&outSR=4326&f=json"
        else:
            url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson"
        '''
        url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson"

        # Property Assessment Data Set
        #RENAME THIS STUFF - Keith
        property_assessment_url = urllib.request.Request(url)
        roads_response = urllib.request.urlopen(property_assessment_url).read().decode("utf-8")
        roads_inventory_json = json_util.loads(roads_response)
        roads_inventory_json = roads_inventory_json['features']

        x = []
        removeEntries = ['SEGMENT_ID', 'L_F_ADD', 'L_T_ADD', 'R_F_ADD', 'R_T_ADD', 'PRE_DIR', 'SUF_DIR', 'CFCC', 'SPEEDLIMIT', 'ONEWAY', 'HEIGHT', 
        'WEIGHT', 'WIDTH', 'F_ZLEV', 'T_ZLEV', 'SHIELD', 'HWY_NUM', 'NBHD_L', 'NBHD_R', 'FT_COST', 'TF_COST', 'TF_DIR', 'FT_DIR', 'STATE00_L', 'STATE00_R', 'COUNTY00_L', 
        'COUNTY00_R', 'PLACE00_L', 'PLACE00_R', 'TRACT00_L', 'TRACT00_R', 'BLOCK00_L', 'BLOCK00_R', 'MCD00_L', 'MCD00_R', 'MCD00_R', 'STREET_ID', 'OBJECTID', 'SHAPElen']

        #Obtains all roads in the Boston region, that have at least some associated street name data, and removes some entries.
        for road in roads_inventory_json:
            if road['properties']['MUN_L'] == 'Boston' or road['properties']['MUN_R'] == 'Boston':
                        # print(road['attributes']['OBJECTID'])
                for entry in removeEntries:
                    road['properties'].pop(entry, None)
                if road['properties'] not in x and '.' not in road['properties']['ST_NAME']:
                    x.append(road['properties'])

        ## IMPORTANT KEYS: Route_ID, Urban_Type, Number_of_Lanes, Street_Name (duh), Length, Toll_Road (nobody likes tolls), struct_cd(?)
        repo.dropCollection("road_segs")
        repo.createCollection("road_segs")
        repo['bkin18_cjoe_klovett_sbrz.road_segs'].insert_many(x)
        repo['bkin18_cjoe_klovett_sbrz.road_segs'].metadata({'complete': True})

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
        doc.add_namespace('ont','http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://gis.massdot.state.ma.us/arcgis/rest/services/')

        this_script = doc.agent('alg:bkin18_cjoe_klovett_sbrz#retrieveRoadsInventory', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:Roads/RoadInventory/MapServer/', {'prov:label': 'Roads Inventory', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_property_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_property_data, this_script)
        doc.usage(get_property_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?where=1%3D1&outFields=*&outSR=4326&f=json'
                  }
                  )

        property_db = doc.entity('dat:bkin18_cjoe_klovett_sbrz#roads_inventory', {prov.model.PROV_LABEL: 'roads_inventory', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(property_db, this_script)
        doc.wasGeneratedBy(property_db,get_property_data, endTime)
        doc.wasDerivedFrom(property_db, resource, get_property_data)

        repo.logout()

        return doc
