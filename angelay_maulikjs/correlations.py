import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from scipy.stats.stats import pearsonr

class correlations(dml.Algorithm):
    contributor = 'angelay_maulikjs'
    reads = ['angelay_maulikjs.clean2012']
    writes = ['angelay_maulikjs.corr2012']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        repo.dropPermanent('angelay_maulikjs')
        repo.createPermanent('angelay_maulikjs')

        l1, l2, l3, l4, l5, l6, l7 = [], [], [], [], [], [], []
        data = repo.angelay_maulikjs.clean2012.find()
        for document in data:
            d = dict(document)
            l1.append(d['CarbonIntensity'])
            l2.append(d['CO2Emissions'])
            l3.append(d['EnergyIntensity'])
            l4.append(d['EnergyUse'])
            l5.append(d['GDPperCapita'])
            l6.append(d['HDI'])
            l7.append(d['Population'])

        CI = pearsonr(l1, l2)[0]
        res = repo.angelay_maulikjs.corr2012.insert_one({'CarbonIntensity':CI})
        EI = pearsonr(l3, l2)[0]
        res = repo.angelay_maulikjs.corr2012.insert_one({'EnergyIntensity':EI})
        EU = pearsonr(l4, l2)[0]
        res = repo.angelay_maulikjs.corr2012.insert_one({'EnergyUse':EU})
        GDP = pearsonr(l5, l2)[0]
        res = repo.angelay_maulikjs.corr2012.insert_one({'GDPperCapita':GDP})
        HDI = pearsonr(l6, l2)[0]
        res = repo.angelay_maulikjs.corr2012.insert_one({'HDI':HDI})
        POP = pearsonr(l7, l2)[0]
        res = repo.angelay_maulikjs.corr2012.insert_one({'Population':POP})

        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}
            

            
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ang', 'http://datamechanics.io/data/angelay_maulikjs/')

        this_script = doc.agent('dat:angelay#correlations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        correlations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Find Correlations between CO2 Emissions and Everything Else', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(correlations, this_script)
        
        resource_clean2012 = doc.entity('dat:angelay#clean2012', {'prov:label':'All Data from 2012 with Outliers Removed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(correlations, resource_clean2012, startTime)

        corr2012 = doc.entity('dat:angelay#corr2012', {'prov:label':'Correlations between CO2 Emissions and Everything Else', prov.model.PROV_TYPE:'ont:Dataset'})

        doc.wasAttributedTo(corr2012, this_script)
        doc.wasGeneratedBy(corr2012, correlations, endTime)
        doc.wasDerivedFrom(corr2012, resource_clean2012, correlations, correlations, correlations)

        repo.logout()

        return doc

#correlations.execute()
