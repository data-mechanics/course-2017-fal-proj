import dml
import prov.model
import datetime
import uuid

class overallScore(dml.Algorithm):

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.transitScore', 'jdbrawn_jliang24_slarbi_tpotye.safetyScore', 'jdbrawn_jliang24_slarbi_tpotye.socialScore']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.ranking']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        transit_score = repo['jdbrawn_jliang24_slarbi_tpotye.transitScore']
        safety_score = repo['jdbrawn_jliang24_slarbi_tpotye.safetyScore']
        social_score = repo['jdbrawn_jliang24_slarbi_tpotye.socialScore']

        ranking = []
        for entry in transit_score.find():
            collegeName = entry['Name']
            transitScore = entry['Transit Score']
            socialEntry = social_score.find_one({"Name": collegeName})
            socialScore = socialEntry['Social Score']
            safetyEntry = safety_score.find_one({"Name": collegeName})
            safetyScore = safetyEntry['Safety Score']

            if transitScore > 1.0: print("Transit Score: " + str(transitScore))
            if socialScore > 1.0: print("Social Score: " + str(socialScore))
            if safetyScore > 1.0: print("Safety Score: " + str(safetyScore))

            overall_score = (transitScore + socialScore + safetyScore) / 3.0

            ranking.append((collegeName, overall_score))

        rankingTemp = sorted(ranking, key=lambda x: x[1], reverse=True)
        ranking = []
        for i in range(len(rankingTemp)):
            ranking.append({'Name': rankingTemp[i][0], 'Score': rankingTemp[i][1], 'Rank': i+1})

        print(ranking)

        repo.dropCollection('ranking')
        repo.createCollection('ranking')
        repo['jdbrawn_jliang24_slarbi_tpotye.ranking'].insert_many(ranking)

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

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#overallScore',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_socialScore = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#socialScore',
                                     {'prov:label': 'Social Score',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_transitScore = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#transitScore',
                                          {'prov:label': 'Transit Score',
                                           prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_safetyScore = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safetyScore',
                                          {'prov:label': 'Safety Score',
                                           prov.model.PROV_TYPE: 'ont:DataSet'})

        get_overallScore = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_overallScore, this_script)

        doc.usage(get_overallScore, resource_socialScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_overallScore, resource_transitScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_overallScore, resource_safetyScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        overall_score = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#overallScore',
                                  {prov.model.PROV_LABEL: 'Overall Score', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(overall_score, this_script)
        doc.wasGeneratedBy(overall_score, get_overallScore, endTime)
        doc.wasDerivedFrom(overall_score, resource_socialScore, get_overallScore, get_overallScore, get_overallScore)
        doc.wasDerivedFrom(overall_score, resource_transitScore, get_overallScore, get_overallScore, get_overallScore)
        doc.wasDerivedFrom(overall_score, resource_safetyScore, get_overallScore, get_overallScore, get_overallScore)

        repo.logout()

        return doc
