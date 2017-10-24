import dml
import prov.model
import datetime
import uuid
import gpxpy.geo

class tAndBike(dml.Algorithm):

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    contributor = 'alankang_xtq'
    reads = ['alankang_xtq.hubway', 'alankang_xtq.MBTA']
    writes = ['alankang_xtq.hubAndMBTA']
    print(456)

    @staticmethod
    def execute(trial=False):
 
        print(111)
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        hubway = repo['alankang_xtq.hubway']
        MBTA = repo['alankang_xtq.MBTA']

        hubwayData = []
        numBikes = []
        MBTAData = []
        print(222)

        for i in hubway.find():
            if 'geometry' in i:
                # latitude, longitude, hub-name, numOfBikes
                hubwayData.append((float(i['geometry']['coordinates'][1]), float(i['geometry']['coordinates'][0]), i['properties']['name']))
                numBikes.append((i['properties']['name'], int(i['properties']['nbBikes'])))
        print(333)
        for i in MBTA.find():
            if 'geometry' in i:
                # latitude, longitude, stop-name
                MBTAData.append((float(i['geometry']['coordinates'][1]), float(i['geometry']['coordinates'][0]), i['properties']['STOP_NAME']))
        print(444)
        hubway_n_stops = []
        for hub in hubwayData:
            for stop in MBTAData:
                if gpxpy.geo.haversine_distance(hub[0], hub[1], stop[0], stop[1]) < 1600:
                    hub_n_stops.append((hub[2], 1))
        hub_n_stops = tAndBike.aggregate(hub_n_stops, sum)
        print(555)
        # where it goes non-trivial
        hubInfo = tAndBike.select(tAndBike.product(school_and_stops, numBikes), lambda t: t[0][0] == t[1][0])
        hub_ranks = tAndBike.project(hubInfo, lambda t: (t[0][0], t[0][1], t[1][1]))
        print(666)
        # put into MongoDB
        processed = []
        for i in hub_ranks:
            processed.append({'Name': i[0], 'Num_of_Stops': i[1], 'Num_of_Bikes': i[2]})
        print(777)
        repo.dropCollection('hubAndMBTA')
        repo.createCollection('hubAndMBTA')
        repo['alankang_xtq.hubAndMBTA'].insert_many(processed)
        print(888)
        repo.logout()
        endTime = datetime.datetime.now()
        print(999)
        return {"start": startTime, "end": endTime}
        print(000)
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
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')

        doc.add_namespace('dio', 'http://datamechanics.io/data/alankang_xtq/')
        doc.add_namespace('cbg', 'https://data.cambridgema.gov/resource/')
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alankang_xtq#tAndBike', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_hubway = doc.entity('dat:alankang_xtq#hubway', {'prov:label': 'hubway', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_MBTA = doc.entity('dat:alankang_xtq#MBTA', {'prov:label': 'MBTA Bus Stops', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_hubAndMBTA = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_hubAndMBTA, this_script)

        doc.usage(get_hubAndMBTA, resource_hubway, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_hubAndMBTA, resource_MBTA, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        MBTA = doc.entity('dat:alankang_xtq#hubAndMBTA', {prov.model.PROV_LABEL: 'MBTA Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(MBTA, this_script)
        doc.wasGeneratedBy(MBTA, get_hubAndMBTA, endTime)
        doc.wasDerivedFrom(MBTA, resource_hubway, get_hubAndMBTA, get_hubAndMBTA, get_hubAndMBTA)
        doc.wasDerivedFrom(MBTA, resource_MBTA, get_hubAndMBTA, get_hubAndMBTA, get_hubAndMBTA)

        repo.logout()

        return doc