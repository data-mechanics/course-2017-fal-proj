import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge2012(dml.Algorithm):
    contributor = 'angelay_maulikjs'
    reads = ['angelay_maulikjs.CarbonIntensity', 'angelay_maulikjs.CO2Emissions', 'angelay_maulikjs.EnergyIntensity', 'angelay_maulikjs.EnergyUse', 'angelay_maulikjs.GDPperCapita', 'angelay_maulikjs.HDI', 'angelay_maulikjs.Population']
    writes = ['angelay_maulikjs.all2012']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        repo.dropPermanent('angelay_maulikjs')
        repo.createPermanent('angelay_maulikjs')

        repo.dropCollection("all2012")
        repo.createCollection("all2012")

        l1, l2, l3, l4, l5, l6, l7 = [], [], [], [], [], [], []
        countries = []

        data = repo.angelay_maulikjs.CarbonIntensity.find_one()
        for key in sorted(data):
            if key != '_id':
                countries.append(key)

        for c in countries:
            item = data[c]['2012']
            l1.append(item)

        data = repo.angelay_maulikjs.CO2Emissions.find_one()
        for c in countries:
            item = data[c]['2012']
            l2.append(item)

        data = repo.angelay_maulikjs.EnergyIntensity.find_one()
        for c in countries:
            item = data[c]['2012']
            l3.append(item)

        data = repo.angelay_maulikjs.EnergyUse.find_one()
        for c in countries:
            item = data[c]['2012']
            l4.append(item)

        data = repo.angelay_maulikjs.GDPperCapita.find_one()
        for c in countries:
            item = data[c]['2012']
            l5.append(item)

        data = repo.angelay_maulikjs.HDI.find_one()
        for c in countries:
            item = data[c]['2012']
            l6.append(item)

        data = repo.angelay_maulikjs.Population.find_one()
        for c in countries:
            item = data[c]['2012']
            l7.append(item)
        
        for i in range(len(countries)):
            entry = {'CarbonIntensity':l1[i], 'CO2Emissions':l2[i], 'EnergyIntensity':l3[i], 'EnergyUse':l4[i], 'GDPperCapita':l5[i], 'HDI':l6[i], 'Population':l7[i]}
            res = repo['angelay_maulikjs.all2012'].insert_one(entry)        

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
        doc.add_namespace('ang', 'http://datamechanics.io/data/angelay/')

        this_script = doc.agent('dat:angelay#merge2012', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        merge2012 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Merge Data from 2012', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(merge2012, this_script)
        
        resource_CarbonIntensity = doc.entity('dat:angelay#CarbonIntensity', {'prov:label':'Carbon Intensity', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_CarbonIntensity, startTime)

        resource_CO2Emissions = doc.entity('dat:angelay#CO2Emissions', {'prov:label':'CO2 Emissions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_CO2Emissions, startTime)

        resource_EnergyIntensity = doc.entity('dat:angelay#EnergyIntensity', {'prov:label':'Energy Intensity', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_EnergyIntensity, startTime)

        resource_EnergyUse = doc.entity('dat:angelay#EnergyUse', {'prov:label':'Energy Use', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_EnergyUse, startTime)

        resource_GDPperCapita = doc.entity('dat:angelay#GDPperCapita', {'prov:label':'GDP per Capita', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_GDPperCapita, startTime)

        resource_HDI = doc.entity('dat:angelay#HDI', {'prov:label':'HDI', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_HDI, startTime)

        resource_Population = doc.entity('dat:angelay#Population', {'prov:label':'Population', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge2012, resource_Population, startTime)

        all2012 = doc.entity('dat:angelay#all2012', {'prov:label':'All Data from 2012', prov.model.PROV_TYPE:'ont:Dataset'})

        doc.wasAttributedTo(all2012, this_script)
        doc.wasGeneratedBy(all2012, merge2012, endTime)
        doc.wasDerivedFrom(all2012, resource_CarbonIntensity, merge2012, merge2012, merge2012)
        doc.wasDerivedFrom(all2012, resource_CO2Emissions, merge2012, merge2012, merge2012)
        doc.wasDerivedFrom(all2012, resource_EnergyIntensity, merge2012, merge2012, merge2012)
        doc.wasDerivedFrom(all2012, resource_EnergyUse, merge2012, merge2012, merge2012)
        doc.wasDerivedFrom(all2012, resource_GDPperCapita, merge2012, merge2012, merge2012)
        doc.wasDerivedFrom(all2012, resource_HDI, merge2012, merge2012, merge2012)
        doc.wasDerivedFrom(all2012, resource_Population, merge2012, merge2012, merge2012)

        repo.logout()

        return doc

#merge2012.execute()
