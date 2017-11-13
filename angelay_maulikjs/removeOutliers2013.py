import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class removeOutliers2013(dml.Algorithm):
    contributor = 'angelay_maulikjs'
    reads = ['angelay_maulikjs.all2013']
    writes = ['angelay_maulikjs.clean2013']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        repo.dropPermanent('angelay_maulikjs')
        repo.createPermanent('angelay_maulikjs')

        l1, l2, l3, l4, l5, l6, l7 = [], [], [], [], [], [], []
        data = repo.angelay_maulikjs.all2013.find()
        for document in data:
            d = dict(document)
            l1.append(d['CarbonIntensity'])
            l2.append(d['CO2Emissions'])
            l3.append(d['EnergyIntensity'])
            l4.append(d['EnergyUse'])
            l5.append(d['GDPperCapita'])
            l6.append(d['HDI'])
            l7.append(d['Population'])

        data = repo.angelay_maulikjs.all2013.find()
        for document in data:
            d = dict(document)
            if removeOutliers2013.isOutlier(d['CarbonIntensity'], l1) or removeOutliers2013.isOutlier(d['CO2Emissions'], l2) or removeOutliers2013.isOutlier(d['EnergyIntensity'], l3) or removeOutliers2013.isOutlier(d['EnergyUse'], l4) or removeOutliers2013.isOutlier(d['GDPperCapita'], l5) or removeOutliers2013.isOutlier(d['HDI'], l6) or removeOutliers2013.isOutlier(d['Population'], l7):
                continue
            else:
                entry = {'CarbonIntensity':d['CarbonIntensity'], 'CO2Emissions':d['CO2Emissions'], 'EnergyIntensity':d['EnergyIntensity'], 'EnergyUse':d['EnergyUse'], 'GDPperCapita':d['GDPperCapita'], 'HDI': d['HDI'], 'Population':d['Population']}
                res = repo.angelay_maulikjs.clean2013.insert_one(entry)
 
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}
            

            
    @staticmethod

    def isOutlier(item, list):
        l = sorted(list)
        iq1 = int(len(l) * 0.25)
        iq3 = int(len(l) * 0.75)
        q1 = l[iq1]
        q3 = l[iq3]
        iqr = q3 - q1
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        return item < low or item > high

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ang', 'http://datamechanics.io/data/angelay_maulikjs/')

        this_script = doc.agent('dat:angelay_maulikjs#removeOutliers2013', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        removeOutliers2013 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Remove Outliers from Merged 2013 Data', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(removeOutliers2013, this_script)
        
        resource_all2013 = doc.entity('dat:angelay_maulikjs#all2013', {'prov:label':'All Data from 2013', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(removeOutliers2013, resource_all2013, startTime)

        clean2013 = doc.entity('dat:angelay_maulikjs#clean2013', {'prov:label':'All Data from 2013 with Outliers Removed', prov.model.PROV_TYPE:'ont:Dataset'})

        doc.wasAttributedTo(clean2013, this_script)
        doc.wasGeneratedBy(clean2013, removeOutliers2013, endTime)
        doc.wasDerivedFrom(clean2013, resource_all2013, removeOutliers2013, removeOutliers2013, removeOutliers2013)

        repo.logout()

        return doc

#removeOutliers2013.execute()
