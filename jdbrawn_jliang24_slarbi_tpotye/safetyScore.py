import dml
import prov.model
import datetime
import uuid

class safetyScore(dml.Algorithm):

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.safetyAnalysis']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.safetyScore']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        safety = repo['jdbrawn_jliang24_slarbi_tpotye.safetyAnalysis']

        score = []
        minCrimes = 99999
        maxCrimes = 0
        minCrashes = 99999
        maxCrashes = 0

        # find mins and maxes
        for entry in safety.find():
            if float(entry['Number of Crimes']) < minCrimes:
                minCrimes = entry['Number of Crimes']
            if float(entry['Number of Crimes']) > maxCrimes:
                maxCrimes = entry['Number of Crimes']
            if float(entry['Number of Crashes']) < minCrashes:
                minCrashes = entry['Number of Crashes']
            if float(entry['Number of Crashes']) > maxCrashes:
                maxCrashes = entry['Number of Crashes']

        crime_max_minus_min = float(maxCrimes - minCrimes)
        crash_max_minus_min = float(maxCrashes - minCrashes)

        # calculate score
        for entry in safety.find():
            crimeScore = float(entry['Number of Crimes'] - minCrimes) / crime_max_minus_min
            crashScore = float(entry['Number of Crashes'] - minCrashes) / crash_max_minus_min
            safetyScore = crimeScore + crashScore / 2.0
            score.append({'Name': entry['Name'], 'Safety Score': safetyScore})

        repo.dropCollection('safetyScore')
        repo.createCollection('safetyScore')
        repo['jdbrawn_jliang24_slarbi_tpotye.safetyScore'].insert_many(score)

        repo.logout()
        endTime = datetime.datetime.now()

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
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_jliang24_slarbi_tpotye/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#safetyScore',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_safety = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safety',
                                       {'prov:label': 'Safety Analysis',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        get_safetyScore = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_safetyScore, this_script)

        doc.usage(get_safetyScore, resource_safety, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        safety_score = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safetyScore',
                            {prov.model.PROV_LABEL: 'Safety Score', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(safety_score, this_script)
        doc.wasGeneratedBy(safety_score, get_safetyScore, endTime)
        doc.wasDerivedFrom(safety_score, resource_safety, get_safetyScore, get_safetyScore, get_safetyScore)

        repo.logout()

        return doc
