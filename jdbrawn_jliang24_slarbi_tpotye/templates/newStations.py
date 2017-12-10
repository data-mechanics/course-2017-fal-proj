import dml
import prov.model
import datetime
import uuid
from sklearn.cluster import KMeans
from tqdm import tqdm


class newStations(dml.Algorithm, k):
    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.safetyScore', 'jdbrawn_jliang24_slarbi_tpotye.colleges']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.newStations']

    
    @staticmethod
    def execute(trial=False):

        NUM_CLUSTERS = k

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        safetyScore = repo['jdbrawn_jliang24_slarbi_tpotye.safetyScore']
        colleges = repo['jdbrawn_jliang24_slarbi_tpotye.colleges']

        # get schools with low safety scores
        P = []
        for entry in safetyScore.find():
            if entry['Safety Score'] < 0.5:
                schoolEntry = colleges.find_one({"Name": entry['Name']})
                P.append((float(schoolEntry['Latitude']), float(schoolEntry['Longitude'])))

        # run k-means on our school locations
        with tqdm(total=100, desc="k-means") as pbar:
            pbar.update(20)
            if not trial:
                kmeans = KMeans(n_clusters=NUM_CLUSTERS)
            else:
                kmeans = KMeans(n_clusters=NUM_CLUSTERS, max_iter=10)
            pbar.update(20)
            kmeans = kmeans.fit(P)
            pbar.update(20)
            labels = kmeans.predict(P)
            pbar.update(20)
            M = kmeans.cluster_centers_
            pbar.update(20)
        print("\nNew Police Station Locations:")
        print(M)
        print()

        # format it for MongoDB
        location_stations = []
        for entry in M:
            location_stations.append({'New Police Location': (entry[0], entry[1])})

        repo.dropCollection('newStations')
        repo.createCollection('newStations')
        repo['jdbrawn_jliang24_slarbi_tpotye.newStations'].insert_many(location_stations)

        repo.logout()
        endTime = datetime.datetime.now()

        return M
        # return {"start": startTime, "end": endTime}

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
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_jliang24_slarbi_tpotye/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#newStations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_safetyScore = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safetyScore',
                                          {'prov:label': 'Safety Scores', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_colleges = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#colleges',
                                       {'prov:label': 'Boston Universities and Colleges',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        get_newStations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_newStations, this_script)

        doc.usage(get_newStations, resource_safetyScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_newStations, resource_colleges, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        newLocation = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#newStations',
                                 {prov.model.PROV_LABEL: 'New Police Stations', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(newLocation, this_script)
        doc.wasGeneratedBy(newLocation, get_newStations, endTime)
        doc.wasDerivedFrom(newLocation, resource_safetyScore, get_newStations, get_newStations, get_newStations)
        doc.wasDerivedFrom(newLocation, resource_colleges, get_newStations, get_newStations, get_newStations)

        repo.logout()

        return doc
