import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class selectlongitudelatandincome(dml.Algorithm):
    contributor = 'lc546_jofranco'
    reads = ['lc546_jofranco.propety']
    writes =['lc546_jofranco.longitudelatitudeincome']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("lc546_jofranco", "lc546_jofranco")
        propertyData = repo.lc546_jofranco.property
        coordinatesdata = propertyData.find()
        coordinate = []
        for location in coordinatesdata:
            coordinate.append(float(location['latitude'],location['longitude']))
        return coordinate

        repo.dropCollection("longitudelatitudeincome")
        repo.createCollection("longitudelatitudeincome")
        repo['lc546_jofranco.longitudelatitudeincome'].insert_many(coordinate)
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lc546_jofranco', 'lc546_jofranco')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/)
        this_script = doc.agent('alg:lc546_jofranco#longitudelatitudeincome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'longitude and latitude properties in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_longandlat= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_longandlat, this_script)
        doc.usage(get_longandlat, resource, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        latitude_longitude = doc.entity('dat:lc546_jofranco#latitude_longitude', {prov.model.PROV_LABEL:'location of household properties in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(latitude_longitude, this_script)
        doc.wasGeneratedBy(latitude_longitude, get_longandlat, endTime)
        doc.wasDerivedFrom(crimerestaurants_intersection, resource, get_longandlat, get_longandlat, get_longandlat)
        # repo.record(doc.serialize()) # Record the provenance document.
        #repo.logout()
        return doc

selectlongitudelatandincome.execute()
doc = selectlongitudelatandincome.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
