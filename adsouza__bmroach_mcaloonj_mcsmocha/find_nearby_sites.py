import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty

'''
Takes the coordinates of speeding complaints submitted to Vision Zero, and
sees if these complaints are within .5 miles of a school, elderly home, or park.
If so, the coordinates of the complaint and the site are inserted into a new dataset.
'''

class find_nearby_sites(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = ['mcaloonj.cleaned_speed_complaints', 'mcaloonj.elderly_homes', 'mcaloonj.schools', 'mcaloonj.open_space']
    writes = ['mcaloonj.sites_near_complaints']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        repo.dropCollection('mcaloonj.sites_near_complaints')
        repo.createCollection('mcaloonj.sites_near_complaints')

        #projection on schools to get dictionary with coordinates as key and name as value
        schools = repo["mcaloonj.schools"].find()
        school_dict = dict()
        for s in schools:
            school_dict[tuple(s["fields"]["geo_shape"]["coordinates"])[::-1]] = s["fields"]["sch_name"]

        #projection on elderly homes to get dictionary with coordinates as key and name as value
        homes = repo["mcaloonj.elderly_homes"].find()
        elderly_dict = dict()
        for h in homes:
            elderly_dict[(h["attributes"]["MatchLatitude"],h["attributes"]["MatchLongitude"])] = h["attributes"]["Project_Name"]

        #projection on parks to get dictionary with first coordinate as key and name as value
        parks = repo["mcaloonj.open_space"].find()
        park_dict = dict()
        for p in parks:
            if p["geometry"]["type"] == "Polygon":
                first_coord = (tuple(p["geometry"]["coordinates"][0][0]))[::-1]
            else:
                first_coord = (tuple(p["geometry"]["coordinates"][0][0][0]))[::-1]
            park_dict[first_coord] = p["properties"]["SITE_NAME"]

        complaints = repo["mcaloonj.cleaned_speed_complaints"].find()
        nearby_sites = []
        for c in complaints:
            coord1 = (c["latitude"], c["longitude"])
            for coord2, name in school_dict.items():
                dist = vincenty(coord1,coord2).miles
                if dist <= .5:
                    nearby_sites.append({"complaint_coordinates": coord1, "site_coordinates": coord2, "name": name, "distance": dist, "comments": c["comments"]})
            for coord2, name in elderly_dict.items():
                dist = vincenty(coord1,coord2).miles
                if dist <= .5:
                    nearby_sites.append({"complaint_coordinates": coord1, "site_coordinates": coord2, "name": name, "distance": dist, "comments": c["comments"]})
            for coord2, name in park_dict.items():
                dist = vincenty(coord1,coord2).miles
                if dist <= .5:
                    nearby_sites.append({"complaint_coordinates": coord1, "site_coordinates": coord2, "name": name, "distance": dist, "comments": c["comments"]})


        repo['mcaloonj.sites_near_complaints'].insert(nearby_sites)
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj','mcaloonj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mcj', 'mcaloonj')

        #Agent
        this_script = doc.agent('alg:mcaloonj#find_nearby_sites', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
        #Resources
        complaints_resource = doc.entity('mcj:cleaned_speed_complaints', {'prov:label': 'Cleaned Speed Complaints', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        elderly_resource = doc.entity('mcj:elderly_homes', {'prov:label': 'Elderly Home Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        schools_resource = doc.entity('mcj:schools', {'prov:label': 'Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})
        parks_resource = doc.entity('mcj:open_space', {'prov:label': 'Open Spaces', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

        #Activities
        get_nearby_sites = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        # Usage
        doc.wasAssociatedWith(get_nearby_sites, this_script)
        doc.usage(get_nearby_sites, complaints_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_nearby_sites, elderly_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_nearby_sites, schools_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_nearby_sites, parks_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        #New dataset
        sites_near_complaints = doc.entity('dat:mcaloonj#sites_near_complaints', {prov.model.PROV_LABEL:'Vulnerable sites near speeding complaints',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sites_near_complaints, this_script)
        doc.wasGeneratedBy(sites_near_complaints, get_nearby_sites, endTime)
        doc.wasDerivedFrom(sites_near_complaints, complaints_resource, get_nearby_sites, get_nearby_sites, get_nearby_sites)
        doc.wasDerivedFrom(sites_near_complaints, elderly_resource, get_nearby_sites, get_nearby_sites, get_nearby_sites)
        doc.wasDerivedFrom(sites_near_complaints, schools_resource, get_nearby_sites, get_nearby_sites, get_nearby_sites)
        doc.wasDerivedFrom(sites_near_complaints, parks_resource, get_nearby_sites, get_nearby_sites, get_nearby_sites)

        repo.logout()
        return doc

'''
find_nearby_sites.execute()
doc = find_nearby_sites.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
##eof
