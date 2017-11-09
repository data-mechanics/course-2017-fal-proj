import dml
import prov.model
import datetime
import uuid

class socialScore(dml.Algorithm):

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.socialAnalysis']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.socialScore']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        social = repo['jdbrawn_jliang24_slarbi_tpotye.socialAnalysis']

        score = []
        minFood = 99999
        maxFood = 0
        minEntertainment = 99999
        maxEntertainment = 0

        # find mins and maxes
        for entry in social.find():
            if float(entry['Number of Food']) < minFood:
                minFood = entry['Number of Food']
            if float(entry['Number of Food']) > maxFood:
                maxFood = entry['Number of Food']
            if float(entry['Number of Entertainment']) < minEntertainment:
                minEntertainment = entry['Number of Entertainment']
            if float(entry['Number of Entertainment']) > maxEntertainment:
                maxEntertainment = entry['Number of Entertainment']

        food_max_minus_min = float(maxFood - minFood)
        entertainment_max_minus_min = float(maxFood - minFood)

        # calculate score
        for entry in social.find():
            foodScore = float(entry['Number of Food'] - minFood) / food_max_minus_min
            entertainmentScore = float(entry['Number of Entertainment'] - minEntertainment) / entertainment_max_minus_min
            socialScore = foodScore + entertainmentScore / 2.0
            score.append({'Name': entry['Name'], 'Social Score': socialScore})

        repo.dropCollection('socialScore')
        repo.createCollection('socialScore')
        repo['jdbrawn_jliang24_slarbi_tpotye.socialScore'].insert_many(score)

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

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#socialScore',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_social = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#social',
                                       {'prov:label': 'Social Analysis',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        get_socialScore = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_socialScore, this_script)

        doc.usage(get_socialScore, resource_social, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        social_score = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#socialScore',
                            {prov.model.PROV_LABEL: 'Social Score', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(social_score, this_script)
        doc.wasGeneratedBy(social_score, get_socialScore, endTime)
        doc.wasDerivedFrom(social_score, resource_social, get_socialScore, get_socialScore, get_socialScore)

        repo.logout()

        return doc