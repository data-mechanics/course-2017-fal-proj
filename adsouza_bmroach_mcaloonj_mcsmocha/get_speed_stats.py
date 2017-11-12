"""
Filename: get_correlation.py

Last edited by: AD and JM 11/12/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Original skeleton files provided by Andrei Lapets (lapets@bu.edu)

Development Notes: 
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from shapely.geometry import Point, LineString
from math import sqrt

class get_speed_stats(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements', 'adsouza_bmroach_mcaloonj_mcsmocha.street_info']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.speed_stats']
    
    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        
        if trial:
            print("in get_statistics.py")
            
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
        repo.dropCollection("speed_stats")
        repo.createCollection("speed_stats")
    
        speed_limits = repo['adsouza_bmroach_mcaloonj_mcsmocha.street_info'].find_one()
        speed_limits = speed_limits['streets']
        signals = repo['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements'].find_one()
        signals = signals['signal_placements']

        # for each sign placed, find the speed limit at that coordinate
        signals_and_limits = []
        smallest_distances = []
        for s in signals:
            distances = []
            # check every street and see if sign is on that street
            for lim in speed_limits:
                sign = Point(s[0], s[1])
                street = LineString([(str[1], str[0]) for str in lim[1]])
                distances.append(street.distance(sign))
                if street.distance(sign) < 1e-8:
                    signals_and_limits.append((s, lim[0]))
                    break
            smallest_distances.append(min(distances))

        # projection: assign 1 for every key value pair
        limit_one = [(l[1], 1) for l in signals_and_limits]
        
        # for every speed limit, find number of signs placed with that speed limit
        limits = {l[0] for l in limit_one}
        limit_frequencies = [(key, sum([v for (k, v) in limit_one if k == key])) for key in limits]
        limit_frequencies = [list(t) for t in zip(*limit_frequencies)]

        def avg(x): # Average
            return sum(x)/len(x)

        def stddev(x): # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

        def cov(x, y): # Covariance.
            return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

        def corr(x, y): # Correlation coefficient.
            if stddev(x)*stddev(y) != 0:
                return cov(x, y)/(stddev(x)*stddev(y))

        correlation = corr(limit_frequencies[0], limit_frequencies[1])
        
        print(correlation)
        
        corr_dict = {'correlation': correlation}
        
        repo['adsouza_bmroach_mcaloonj_mcsmocha.speed_stats'].insert_one(corr_dict)
        repo['adsouza_bmroach_mcaloonj_mcsmocha.speed_stats'].metadata({'complete':True})
        
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
        doc.add_namespace('dbg','https://data.boston.gov')

        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#get_signal_placements', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Signal Placements', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

        get_signal_placements = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_signal_placements, this_script)

        doc.usage(get_signal_placements, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'6222085d-ee88-45c6-ae40-0c7464620d64'
                  }
                  )

        signal_placements = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#signal_placements', {prov.model.PROV_LABEL:'Signal Placements',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(signal_placements, this_script)
        doc.wasGeneratedBy(signal_placements, get_signal_placements, endTime)
        doc.wasDerivedFrom(signal_placements, resource, get_signal_placements, get_signal_placements, get_signal_placements)

        repo.logout()
        return doc
            
# get_speed_stats.execute()
# doc = get_signal_placements.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))