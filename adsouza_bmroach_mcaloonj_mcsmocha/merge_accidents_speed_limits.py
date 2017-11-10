import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

'''
For each street, get speed limit and number of accidents
'''
class merge_accidents_speed_limits(dml.Algorithm):
    contributor = 'adsouza_bmroach_mcaloonj_mcsmocha'
    reads = ['adsouza_bmroach_mcaloonj_mcsmocha.accidents', 'adsouza_bmroach_mcaloonj_mcsmocha.speed_limits']
    writes = ['adsouza_bmroach_mcaloonj_mcsmocha.merged_accidents_speed_limits']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')

        repo.dropCollection('adsouza_bmroach_mcaloonj_mcsmocha.merged_accidents_speed_limits')
        repo.createCollection('adsouza_bmroach_mcaloonj_mcsmocha.merged_accidents_speed_limits')

        accidents = repo["adsouza_bmroach_mcaloonj_mcsmocha.accidents"].find()
        streets = [a["STREET"] for a in accidents]

        #aggsum street name and number of accidents to get tuples of form (st name, number of accidents)
        keys = set(streets)

        accidents_per_street = sorted([(key, sum([1 for street in streets if street == key])) for key in keys], key=lambda tup: tup[1], reverse=True)[1:]

        accidents_per_street = dict(accidents_per_street)

        #print (accidents_per_street)
        speed_limits = repo["adsouza_bmroach_mcaloonj_mcsmocha.speed_limits"].find()

        #project to get tuples of form (st name, speed limit)
        cleaned_speed_limits = set()
        for x in speed_limits:
            try:
                st_name = (x["properties"]["ST_NAME"]).upper()
                st_type = (x["properties"]["ST_TYPE"]).upper()
                s_limit = x["properties"]["SPEEDLIMIT"]
                cleaned_speed_limits.add((st_name + ' ' + st_type, s_limit))
            except:
                None

        cleaned_speed_limits = dict(cleaned_speed_limits)

        #finally get street, num_accidents, speed_limit
        combined = []
        for s,a in accidents_per_street.items():
            if s in cleaned_speed_limits:
                combined.append({"street":s,"num_accidents":a,"speed_limit":cleaned_speed_limits[s]})

        repo['adsouza_bmroach_mcaloonj_mcsmocha.merged_accidents_speed_limits'].insert_many(combined)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha','adsouza_bmroach_mcaloonj_mcsmocha')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mcj', 'adsouza_bmroach_mcaloonj_mcsmocha')

        #Agent
        this_script = doc.agent('alg:adsouza_bmroach_mcaloonj_mcsmocha#merge_accidents_speed_limits', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

        #Resources
        accidents_resource = doc.entity('mcj:accidents', {'prov:label': 'Accidents', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        speedlimit_resource = doc.entity('mcj:speed_limits',{'prov:label': 'Speed Limits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

        # Activities
        merge_accidents_speed_limits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        # Usage
        doc.wasAssociatedWith(merge_accidents_speed_limits, this_script)

        doc.usage(merge_accidents_speed_limits, accidents_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.usage(merge_accidents_speed_limits, speedlimit_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        # New dataset
        merged_accidents_speed_limits = doc.entity('dat:adsouza_bmroach_mcaloonj_mcsmocha#merged_accidents_speed_limits', {prov.model.PROV_LABEL:'Streets with number of accidents and speed limit', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(merged_accidents_speed_limits, this_script)
        doc.wasGeneratedBy(merged_accidents_speed_limits, merge_accidents_speed_limits, endTime)
        doc.wasDerivedFrom(merged_accidents_speed_limits, accidents_resource, merge_accidents_speed_limits, merge_accidents_speed_limits, merge_accidents_speed_limits)
        doc.wasDerivedFrom(merged_accidents_speed_limits, speedlimit_resource, merge_accidents_speed_limits, merge_accidents_speed_limits, merge_accidents_speed_limits)

        repo.logout()

        return doc

'''
merge_accidents_speed_limits.execute()
doc = merge_accidents_speed_limits.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
