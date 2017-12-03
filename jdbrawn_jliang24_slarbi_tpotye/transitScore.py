import dml
import prov.model
import datetime
import uuid

class transitScore(dml.Algorithm):

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.mbtaAnalysis']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.transitScore']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        transit = repo['jdbrawn_jliang24_slarbi_tpotye.mbtaAnalysis']

        score = []
        minStops = 99999
        maxStops = 0

        # find mins and maxes
        for entry in transit.find():
            if float(entry['Number of MBTA stops']) < minStops:
                minStops = entry['Number of MBTA stops']
            if float(entry['Number of MBTA stops']) > maxStops:
                maxStops = entry['Number of MBTA stops']

        transit_max_minus_min = float(maxStops - minStops)

        # calculate score
        for entry in transit.find():
            transit_score = float(entry['Number of MBTA stops'] - minStops) / transit_max_minus_min
            score.append({'Name': entry['Name'], 'Transit Score': transit_score})

        repo.dropCollection('transitScore')
        repo.createCollection('transitScore')
        repo['jdbrawn_jliang24_slarbi_tpotye.transitScore'].insert_many(score)

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

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#transitScore',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_transit = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#transit',
                                     {'prov:label': 'Transit Analysis',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        get_transitScore = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_transitScore, this_script)

        doc.usage(get_transitScore, resource_transit, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        transit_score = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#transitScore',
                                  {prov.model.PROV_LABEL: 'Transit Score', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(transit_score, this_script)
        doc.wasGeneratedBy(transit_score, get_transitScore, endTime)
        doc.wasDerivedFrom(transit_score, resource_transit, get_transitScore, get_transitScore, get_transitScore)

        repo.logout()

        return doc