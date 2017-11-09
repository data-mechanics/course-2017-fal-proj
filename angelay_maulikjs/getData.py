import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getData(dml.Algorithm):
    contributor = 'angelay_maulikjs'
    reads = []
    writes = ['angelay_maulikjs.CarbonIntensity', 'angelay_maulikjs.CO2Emissions', 'angelay_maulikjs.EnergyIntensity', 'angelay_maulikjs.EnergyUse', 'angelay_maulikjs.GDPperCapita', 'angelay_maulikjs.HDI', 'angelay_maulikjs.Population']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        url = 'http://datamechanics.io/data/angelay/CarbonIntensity.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("CarbonIntensity")
        repo.createCollection("CarbonIntensity")
        repo['angelay_maulikjs.CarbonIntensity'].insert(r)
        repo['angelay_maulikjs.CarbonIntensity'].metadata({'complete':True})
        print(repo['angelay_maulikjs.CarbonIntensity'].metadata())

        url = 'http://datamechanics.io/data/angelay/CO2Emissions.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("CO2Emissions")
        repo.createCollection("CO2Emissions")
        repo['angelay_maulikjs.CO2Emissions'].insert(r)
        repo['angelay_maulikjs.CO2Emissions'].metadata({'complete':True})
        print(repo['angelay_maulikjs.CO2Emissions'].metadata())

        url = 'http://datamechanics.io/data/angelay/EnergyIntensity.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("EnergyIntensity")
        repo.createCollection("EnergyIntensity")
        repo['angelay_maulikjs.EnergyIntensity'].insert(r)
        repo['angelay_maulikjs.EnergyIntensity'].metadata({'complete':True})
        print(repo['angelay_maulikjs.EnergyIntensity'].metadata())

        url = 'http://datamechanics.io/data/angelay/EnergyUse.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("EnergyUse")
        repo.createCollection("EnergyUse")
        repo['angelay_maulikjs.EnergyUse'].insert(r)
        repo['angelay_maulikjs.EnergyUse'].metadata({'complete':True})
        print(repo['angelay_maulikjs.EnergyUse'].metadata())

        url = 'http://datamechanics.io/data/angelay/GDPperCapita.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("GDPperCapita")
        repo.createCollection("GDPperCapita")
        repo['angelay_maulikjs.GDPperCapita'].insert(r)
        repo['angelay_maulikjs.GDPperCapita'].metadata({'complete':True})
        print(repo['angelay_maulikjs.GDPperCapita'].metadata())

        url = 'http://datamechanics.io/data/angelay/HDI.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("HDI")
        repo.createCollection("HDI")
        repo['angelay_maulikjs.HDI'].insert(r)
        repo['angelay_maulikjs.HDI'].metadata({'complete':True})
        print(repo['angelay_maulikjs.HDI'].metadata())

        url = 'http://datamechanics.io/data/angelay/Population.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Population")
        repo.createCollection("Population")
        repo['angelay_maulikjs.Population'].insert(r)
        repo['angelay_maulikjs.Population'].metadata({'complete':True})
        print(repo['angelay_maulikjs.Population'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ang', 'http://datamechanics.io/data/angelay/')

        this_script = doc.agent('dat:angelay#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_CarbonIntensity = doc.entity('ang:CarbonIntensity', {'prov:label':'Carbon Intensity', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_CarbonIntensity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Carbon Intensity', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_CarbonIntensity, this_script)
        doc.usage(get_CarbonIntensity, resource_CarbonIntensity, startTime)

        resource_CO2Emissions = doc.entity('ang:CO2Emissions', {'prov:label':'CO2 Emissions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_CO2Emissions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get CO2 Emissions', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_CO2Emissions, this_script)
        doc.usage(get_CO2Emissions, resource_CO2Emissions, startTime)

        resource_EnergyIntensity = doc.entity('ang:EnergyIntensity', {'prov:label':'Energy Intensity', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_EnergyIntensity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Energy Intensity', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_EnergyIntensity, this_script)
        doc.usage(get_EnergyIntensity, resource_EnergyIntensity, startTime)

        resource_EnergyUse = doc.entity('ang:EnergyUse', {'prov:label':'Energy Use', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_EnergyUse = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Energy Use', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_EnergyUse, this_script)
        doc.usage(get_EnergyUse, resource_EnergyUse, startTime)

        resource_GDPperCapita = doc.entity('ang:GDPperCapita', {'prov:label':'GDP per Capita', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_GDPperCapita = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get GDP per Capita', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_GDPperCapita, this_script)
        doc.usage(get_GDPperCapita, resource_GDPperCapita, startTime)

        resource_HDI = doc.entity('ang:HDI', {'prov:label':'HDI', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_HDI = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get HDI', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_HDI, this_script)
        doc.usage(get_HDI, resource_HDI, startTime)

        resource_Population = doc.entity('ang:Population', {'prov:label':'Population', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Population = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Population', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_Population, this_script)
        doc.usage(get_Population, resource_Population, startTime)



        CarbonIntensity = doc.entity('dat:angelay#CarbonIntensity', {prov.model.PROV_LABEL:'Carbon Intensity', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CarbonIntensity, this_script)
        doc.wasGeneratedBy(CarbonIntensity, get_CarbonIntensity, endTime)
        doc.wasDerivedFrom(CarbonIntensity, resource_CarbonIntensity, get_CarbonIntensity, get_CarbonIntensity, get_CarbonIntensity)

        CO2Emissions = doc.entity('dat:angelay#CO2Emissions', {prov.model.PROV_LABEL:'CO2 Emissions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CO2Emissions, this_script)
        doc.wasGeneratedBy(CO2Emissions, get_CO2Emissions, endTime)
        doc.wasDerivedFrom(CO2Emissions, resource_CO2Emissions, get_CO2Emissions, get_CO2Emissions, get_CO2Emissions)

        EnergyIntensity = doc.entity('dat:angelay#EnergyIntensity', {prov.model.PROV_LABEL:'Energy Intensity', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(EnergyIntensity, this_script)
        doc.wasGeneratedBy(EnergyIntensity, EnergyIntensity, endTime)
        doc.wasDerivedFrom(EnergyIntensity, resource_EnergyIntensity, get_EnergyIntensity, get_EnergyIntensity, get_EnergyIntensity)

        EnergyUse = doc.entity('dat:angelay#EnergyUse', {prov.model.PROV_LABEL:'Energy Use', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(EnergyUse, this_script)
        doc.wasGeneratedBy(EnergyUse, get_EnergyUse, endTime)
        doc.wasDerivedFrom(EnergyUse, resource_EnergyUse, get_EnergyUse, get_EnergyUse, get_EnergyUse)

        GDPperCapita = doc.entity('dat:angelay#GDPperCapita', {prov.model.PROV_LABEL:'GDP per Capita', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(GDPperCapita, this_script)
        doc.wasGeneratedBy(GDPperCapita, get_GDPperCapita, endTime)
        doc.wasDerivedFrom(GDPperCapita, resource_GDPperCapita, get_GDPperCapita, get_GDPperCapita, get_GDPperCapita)

        HDI = doc.entity('dat:angelay#HDI', {prov.model.PROV_LABEL:'HDI', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(HDI, this_script)
        doc.wasGeneratedBy(HDI, get_HDI, endTime)
        doc.wasDerivedFrom(HDI, resource_HDI, get_HDI, get_HDI, get_HDI)

        Population = doc.entity('dat:angelay#Population', {prov.model.PROV_LABEL:'Population', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Population, this_script)
        doc.wasGeneratedBy(Population, get_Population, endTime)
        doc.wasDerivedFrom(Population, resource_Population, get_Population, get_Population, get_Population)

        repo.logout()

        return doc
'''
getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
