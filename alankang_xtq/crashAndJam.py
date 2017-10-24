import dml
import prov.model
import datetime
import uuid

class crashAndJam(dml.Algorithm):

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
    reads = ['alankang_xtq.crush', 'alankang_xtq.jam']
    writes = ['alankang_xtq.crushAndJam']

    @staticmethod
    def execute(trial=False):
 
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        crash = repo['alankang_xtq.crash']
        jam = repo['alankang_xtq.jam']

        crashData = []
        jamData = []

        for i in crash.find():
            if 'cross_street' in i:
                # street name, 
                crashData.append((i['steet_name'], 1))
                crashData.append((i['cross_street'], 1))

        for i in jam.find():
            if 'city' in i:
                # street name, delay time
                jamData.append((i['street'], int(i['delay'])))

        crashes = crashData.aggregate(crashData, sum)
        jams = jamData.aggregate(jamData, sum)

        # where it goes non-trivial
        crash_n_jam = select(product(crashes,jams), lambda t: t[0][0] == t[1][0])

        # put into MongoDB
        processed = []
        for i in crash_n_jam:
            processed.append({'Street Name': i[0], 'crashes': i[1], 'jam delay': i[2]})

        repo.dropCollection('crash_and_jam')
        repo.createCollection('crash_and_jam')
        repo['alankang_xtq.crash_and_jam'].insert_many(processed)

        repo.logout()
        endTime = datetime.datetime.now()
        print(999)
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
        repo.authenticate('alankang_xtq', 'alankang_xtq')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')

        doc.add_namespace('dio', 'http://datamechanics.io/data/alankang_xtq/')
        doc.add_namespace('cbg', 'https://data.cambridgema.gov/resource/')
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alankang_xtq#crashAndJam', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_crash = doc.entity('dat:alankang_xtq#crash', {'prov:label': 'crash', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_jam = doc.entity('dat:alankang_xtq#jam', {'prov:label': 'jam', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crash_and_jam = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crash_and_jam, this_script)

        doc.usage(get_crash_and_jam, resource_crash, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_crash_and_jam, resource_jam, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        crash_and_jam = doc.entity('dat:alankang_xtq#crashAndJam', {prov.model.PROV_LABEL: 'crash and jam analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_and_jam, this_script)
        doc.wasGeneratedBy(crash_and_jam, get_crash_and_jam, endTime)
        doc.wasDerivedFrom(crash_and_jam, resource_crash, get_crash_and_jam, get_crash_and_jam, get_crash_and_jam)
        doc.wasDerivedFrom(crash_and_jam, resource_jam, get_crash_and_jam, get_crash_and_jam, get_crash_and_jam)

        repo.logout()

        return doc