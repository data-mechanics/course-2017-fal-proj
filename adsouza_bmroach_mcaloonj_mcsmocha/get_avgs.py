import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty
from math import sqrt

class get_avgs(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers', 'adsouza_bmroach_mcaloonj_mcsmocha.signal_placements']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats']

    @staticmethod
    def execute(trial=False, logging=True):
        startTime = datetime.datetime.now()

        if logging:
            print("in get_avgs.py")


        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        signals = repo['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements'].find_one()
        signals = signals["signal_placements"]

        clean_triggers = repo['adsouza_bmroach_mcaloonj_mcsmocha.clean_triggers'].find_one()
        schools = clean_triggers["schools"]
        hospitals = clean_triggers["hospitals"]
        parks = clean_triggers["parks"]
        accident_clusters = clean_triggers["accident_clusters"]
        num_clusters = len(accident_clusters)
        print (num_clusters)

        def avg(x): # Average
            return sum(x)/len(x)

        def stddev(x): # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

        def get_min_dists(triggers):
            distances = []
            for signal in signals:
                min_dist = float('inf')

                for t in triggers:
                    dist = vincenty(t,signal).miles
                    if dist < min_dist:
                        min_dist = dist
                distances.append(min_dist)
            return distances


        sch_distances = get_min_dists(schools)
        hosp_distances = get_min_dists(hospitals)
        park_distances = get_min_dists(parks)
        acc_clus_distances = get_min_dists(accident_clusters)

        avg_sch = avg(sch_distances)
        avg_hosp = avg(hosp_distances)
        avg_park = avg(park_distances)
        avg_acc = avg(acc_clus_distances)

        # print ("Schools:", avg_sch)
        # print ()
        # print ("Hospitals", avg_hosp)
        # print ()
        # print ("Parks", avg_park)
        # print ()
        # print ("Accident Clusters", avg_acc)
        # print ()

        # std_sch = stddev(sch_distances)
        # std_hosp = stddev(hosp_distances)
        # std_park = stddev(park_distances)
        # std_acc = stddev(acc_clus_distances)

        # print ("Schools:", std_sch)
        # print ()
        # print ("Hospitals", std_hosp)
        # print ()
        # print ("Parks", std_park)
        # print ()
        # print ("Accident Clusters", std_acc)

        stats_dict = dict()
        stats_dict[str(num_clusters)] = [avg_sch, avg_hosp, avg_park, avg_acc]

        #repo.dropCollection("num_clusters_stats") #if you want to get rid of all previous k values
        repo.createCollection("num_clusters_stats")


        repo['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats'].update(stats_dict, stats_dict, upsert= True)
        repo['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats'].metadata({'complete':True})
        # stuff = repo['adsouza_bmroach_mcaloonj_mcsmocha.num_clusters_stats'].find()
        # for s in stuff:
        #     print (s)
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dat','adsouza_bmroach_mcaloonj_mcsmocha#clean_triggers')
        doc.add_namespace('dat','adsouza_bmroach_mcaloonj_mcsmocha#signal_placements')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#get_avgs', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

        #Resources
        clean_triggers = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#clean_triggers', {'prov:label': 'Clean Triggers', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        signal_placements = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#signal_placements', {'prov:label': 'Signal Placements', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})


        get_avgs = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_avgs, this_script)

        doc.usage(get_avgs, clean_triggers, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'6222085d-ee88-45c6-ae40-0c7464620d64'
                  }
                  )

        doc.usage(get_avgs, signal_placements, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'6222085d-ee88-45c6-ae40-0c7464620d64'
                  }
                  )

        school_stats = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#school_stats', {prov.model.PROV_LABEL:'School Statistics',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school_stats, this_script)
        doc.wasGeneratedBy(school_stats, school_stats, endTime)
        doc.wasDerivedFrom(school_stats, clean_triggers, get_avgs, get_avgs, get_avgs)
        doc.wasDerivedFrom(school_stats, signal_placements, get_avgs, get_avgs, get_avgs)

        repo.logout()
        return doc


#get_avgs.execute()
#doc = get_avgs.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
