import dml
import prov.model
import datetime
import uuid
from tqdm import tqdm

class rankingOptimizer(dml.Algorithm):

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.transitScore', 'jdbrawn_jliang24_slarbi_tpotye.safetyScore', 'jdbrawn_jliang24_slarbi_tpotye.socialScore', 'jdbrawn_jliang24_slarbi_tpotye.ranking']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.optimizedRanking', 'jdbrawn_jliang24_slarbi_tpotye.optimizedRankingStats']

    @staticmethod
    def execute(trial=False):

        SCHOOL_NAME = 'Boston University School of Medicine'
        #SCHOOL_NAME = 'Boston College'

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        transit_score = repo['jdbrawn_jliang24_slarbi_tpotye.transitScore']
        safety_score = repo['jdbrawn_jliang24_slarbi_tpotye.safetyScore']
        social_score = repo['jdbrawn_jliang24_slarbi_tpotye.socialScore']
        overall_score = repo['jdbrawn_jliang24_slarbi_tpotye.ranking']

        currentRankingEntry = overall_score.find_one({'Name':SCHOOL_NAME})
        currentRanking = currentRankingEntry['Rank']
        optimized_ranking = []
        maxRanking = currentRanking
        finalTransitWeight = 100/3
        finalSocialWeight = 100/3
        finalSafetyWeight = 100/3
        improvedRanking = False

        if not trial:
            for transitWeight in tqdm(range(20, 51)):
                for safetyWeight in range(20, 51):
                    if transitWeight + safetyWeight <= 80 and (100 - (transitWeight + safetyWeight)) < 51:
                        socialWeight = 100 - (transitWeight + safetyWeight)

                        tempRanking = []
                        for entry in transit_score.find():

                            collegeName = entry['Name']
                            transitScore = entry['Transit Score']
                            socialEntry = social_score.find_one({"Name": collegeName})
                            socialScore = socialEntry['Social Score']
                            safetyEntry = safety_score.find_one({"Name": collegeName})
                            safetyScore = safetyEntry['Safety Score']

                            score = (transitScore * transitWeight/100) + (socialScore * socialWeight/100) + (safetyScore * safetyWeight/100)

                            tempRanking.append((collegeName, score))

                        tempRankingSorted = sorted(tempRanking, key=lambda x: x[1], reverse=True)
                        for i in range(len(tempRankingSorted)):
                            if tempRankingSorted[i][0] == SCHOOL_NAME:
                                if i+1 < maxRanking:
                                    improvedRanking = True
                                    maxRanking = i+1
                                    optimized_ranking = tempRankingSorted
                                    finalSafetyWeight = safetyWeight
                                    finalSocialWeight = socialWeight
                                    finalTransitWeight = transitWeight

        else:
            for transitWeight in tqdm(range(20, 51, 2)):
                for safetyWeight in range(20, 51, 2):
                    if transitWeight + safetyWeight <= 80 and (100 - (transitWeight + safetyWeight)) < 51:
                        socialWeight = 100 - (transitWeight + safetyWeight)

                        tempRanking = []
                        found = False
                        limit = 10

                        for entry in transit_score.find():
                            if limit == 0:
                                break

                            collegeName = entry['Name']
                            if collegeName == SCHOOL_NAME:
                                found = True

                            transitScore = entry['Transit Score']
                            socialEntry = social_score.find_one({"Name": collegeName})
                            socialScore = socialEntry['Social Score']
                            safetyEntry = safety_score.find_one({"Name": collegeName})
                            safetyScore = safetyEntry['Safety Score']

                            score = (transitScore * transitWeight/100) + (socialScore * socialWeight/100) + (safetyScore * safetyWeight/100)

                            tempRanking.append((collegeName, score))
                            limit = limit - 1

                        if not found:
                            transitEntry = transit_score.find_one({"Name": SCHOOL_NAME})
                            transitScore = transitEntry['Transit Score']
                            socialEntry = social_score.find_one({"Name": SCHOOL_NAME})
                            socialScore = socialEntry['Social Score']
                            safetyEntry = safety_score.find_one({"Name": SCHOOL_NAME})
                            safetyScore = safetyEntry['Safety Score']

                            score = (transitScore * transitWeight / 100) + (socialScore * socialWeight / 100) + (
                            safetyScore * safetyWeight / 100)

                            tempRanking.append((SCHOOL_NAME, score))

                        tempRankingSorted = sorted(tempRanking, key=lambda x: x[1], reverse=True)
                        for i in range(len(tempRankingSorted)):
                            if tempRankingSorted[i][0] == SCHOOL_NAME:
                                if i+1 < maxRanking:
                                    improvedRanking = True
                                    maxRanking = i+1
                                    optimized_ranking = tempRankingSorted
                                    finalSafetyWeight = safetyWeight
                                    finalSocialWeight = socialWeight
                                    finalTransitWeight = transitWeight

        #print(optimized_ranking)
        print("\nOptimized Ranking for " + SCHOOL_NAME)
        print("Original Ranking: " + str(currentRanking))
        print("Max Ranking: " + str(maxRanking))
        print("New Transit Weight: " + str(finalTransitWeight))
        print("New Safety Weight: " + str(finalSafetyWeight))
        print("New Social Weight: " + str(finalSocialWeight) + "\n")

        if improvedRanking:
            finalRanking = []
            for i in range(len(optimized_ranking)):
                finalRanking.append({'Name': optimized_ranking[i][0], 'Score': optimized_ranking[i][1], 'Rank': i+1})
        else:
            originalRanking = []
            for entry in overall_score.find():
                originalRanking.append(entry)
            finalRanking = originalRanking

        #print(finalRanking)

        repo.dropCollection('optimizedRanking')
        repo.createCollection('optimizedRanking')
        repo['jdbrawn_jliang24_slarbi_tpotye.optimizedRanking'].insert_many(finalRanking)

        finalStats = [({'Name': SCHOOL_NAME, 'Original Ranking': currentRanking, 'Max Ranking': maxRanking, 'Transit Weight': finalTransitWeight,
                       'Safety Weight': finalSafetyWeight, 'Social Weight': finalSocialWeight})]

        repo.dropCollection('optimizedRankingStats')
        repo.createCollection('optimizedRankingStats')
        repo['jdbrawn_jliang24_slarbi_tpotye.optimizedRankingStats'].insert_many(finalStats)

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

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#rankingOptimizer',
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
        resource_ranking = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#ranking',
                                          {'prov:label': 'Overall Score',
                                           prov.model.PROV_TYPE: 'ont:DataSet'})

        get_optimalRanking = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_optimalRanking, this_script)

        doc.usage(get_optimalRanking, resource_socialScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_optimalRanking, resource_transitScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_optimalRanking, resource_safetyScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_optimalRanking, resource_ranking, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        optimized_ranking = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#optimizedRanking',
                                   {prov.model.PROV_LABEL: 'Optimized Ranking', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(optimized_ranking, this_script)
        doc.wasGeneratedBy(optimized_ranking, get_optimalRanking, endTime)
        doc.wasDerivedFrom(optimized_ranking, resource_socialScore, get_optimalRanking, get_optimalRanking, get_optimalRanking)
        doc.wasDerivedFrom(optimized_ranking, resource_transitScore, get_optimalRanking, get_optimalRanking, get_optimalRanking)
        doc.wasDerivedFrom(optimized_ranking, resource_safetyScore, get_optimalRanking, get_optimalRanking, get_optimalRanking)
        doc.wasDerivedFrom(optimized_ranking, resource_ranking, get_optimalRanking, get_optimalRanking, get_optimalRanking)


        get_optimalRankingStats = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_optimalRankingStats, this_script)

        doc.usage(get_optimalRankingStats, resource_socialScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_optimalRankingStats, resource_transitScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_optimalRankingStats, resource_safetyScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_optimalRankingStats, resource_ranking, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        optimized_ranking_stats = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#optimizedRankingStats',
                                       {prov.model.PROV_LABEL: 'Optimized Ranking Stats',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(optimized_ranking_stats, this_script)
        doc.wasGeneratedBy(optimized_ranking_stats, get_optimalRankingStats, endTime)
        doc.wasDerivedFrom(optimized_ranking_stats, resource_socialScore, get_optimalRankingStats, get_optimalRankingStats,
                           get_optimalRankingStats)
        doc.wasDerivedFrom(optimized_ranking_stats, resource_transitScore, get_optimalRankingStats, get_optimalRankingStats,
                           get_optimalRankingStats)
        doc.wasDerivedFrom(optimized_ranking_stats, resource_safetyScore, get_optimalRankingStats, get_optimalRankingStats,
                           get_optimalRankingStats)
        doc.wasDerivedFrom(optimized_ranking_stats, resource_ranking, get_optimalRankingStats, get_optimalRankingStats,
                           get_optimalRankingStats)

        repo.logout()

        return doc

