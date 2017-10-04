import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests


class crimeWeatherTransformation(dml.Algorithm):
    def union(R, S):
        return R + S
    def intersect(R, S):
        return [t for t in R if t in S]
    def product(R, S):
        return [(t,u) for t in R for u in S]
    def select(R, s):
        return [t for t in R if s(t)]
    def aggregate(R):
        keys = {r[0] for r in R}
        return [(key, [v for (k,v) in R if k == key]) for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    def removeDuplicates(seq):
        #helper function from previous semester
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x)) and x != " "]

    def betweenDates(t):
        startDate = datetime.datetime(2016, 1, 1).date()
        endDate = datetime.datetime(2017, 1, 1).date()
        date = datetime.datetime.strptime(t['OCCURRED_ON_DATE'], '%Y-%m-%dT%H:%M:%S').date()
        return (startDate <= date <= endDate)

    def equalDates(t):
        crimeDate = datetime.datetime.strptime(t[1]['OCCURRED_ON_DATE'], '%Y-%m-%dT%H:%M:%S')
        stormDate = datetime.datetime.strptime(t[0]['BEGIN_DATE'], '%m/%d/%Y')
        return (crimeDate.date() == stormDate.date())

    def aggregateByDate(R):
        keys = {r['OCCURRED_ON_DATE'] for r in R}
        return [(key, [v for (k,v) in R if k == key]) for key in keys]


    contributor = 'rooday_shreyapandit'
    reads = ['rooday_shreyapandit.crime',
              'rooday_shreyapandit.weather']
    writes = ['rooday_shreyapandit.crimeWeatherAnalysis']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        crimeData = repo['rooday_shreyapandit.crime']
        weatherData = repo['rooday_shreyapandit.weather']

        #print(crimeData.find()[0])
        #print(datetime.datetime.strptime(crimeData[0]['OCCURRED_ON_DATE'], '%Y-%m-%dT%H:%M:%S'))
        #print(datetime.datetime.strptime(crimeData[0]['OCCURRED_ON_DATE'], '%Y-%m-%dT%H:%M:%S').date())
        #print(weatherData.find()[0])

        crimes2016 = crimeWeatherTransformation.select(crimeData.find(), crimeWeatherTransformation.betweenDates)
        stormsAndCrimes = crimeWeatherTransformation.select(crimeWeatherTransformation.product(weatherData.find(), crimeData.find()), crimeWeatherTransformation.equalDates)

        finalList = []
        dates = []

        for entry in crimes2016:
            dates.append(datetime.datetime.strptime(entry['OCCURRED_ON_DATE'], '%Y-%m-%dT%H:%M:%S').date())

        dates = crimeWeatherTransformation.removeDuplicates(dates)

        test = crimeWeatherTransformation.aggregateByDate(crimes2016)
        print(test[0])

        #print(crimes2016[0])
        #print(stormsAndCrimes[0])


        #begin transformation
        
        '''foodZips = []
        entertainmentZips = []
        finalList = []
        for entry in foodLoc.find():
            if 'zip' in entry:
                foodZips.append((entry['zip'], entry['businessname']))

        for entry in entertainmentLoc.find():
            if 'zip' in entry:
                entertainmentZips.append((entry['zip'], entry['businessname']))
#            entertainmentZips.append({'zipcode': entry['zip'], "name": entry['businessname']})

        both = crimeWeatherTransformation.union(foodZips, entertainmentZips)
        both = crimeWeatherTransformation.removeDuplicates(both)
        combo = crimeWeatherTransformation.aggregate(both)
        for entry in combo:
            finalList.append({'zipcode':entry[0], 'numSocialBusinesses':len(entry[1])})
        print(finalList)
            



        print('DONE!')
        repo.dropCollection('crimeWeatherAnalysis')
        repo.createCollection('crimeWeatherAnalysis')
        repo['rooday_shreyapandit.crimeWeatherAnalysis'].insert_many(finalList)
'''
        
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/rooday_shreyapandit/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')


        this_script = doc.agent('alg:rooday_shreyapandit#crimeWeatherTransformation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_entertain = doc.entity('dat:rooday_shreyapandit#entertain', {'prov:label':'Entertainment Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_food = doc.entity('dat:rooday_shreyapandit#food', {'prov:label': 'Food Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crimeWeatherAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crimeWeatherAnalysis, this_script)

        doc.usage(get_crimeWeatherAnalysis, resource_entertain, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_crimeWeatherAnalysis, resource_food, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        social = doc.entity('dat:rooday_shreyapandit#crimeWeatherAnalysis', {prov.model.PROV_LABEL: 'Social Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(social, this_script)
        doc.wasGeneratedBy(social, get_crimeWeatherAnalysis, endTime)
        doc.wasDerivedFrom(social, resource_entertain, get_crimeWeatherAnalysis, get_crimeWeatherAnalysis, get_crimeWeatherAnalysis)
        doc.wasDerivedFrom(social, resource_food, get_crimeWeatherAnalysis, get_crimeWeatherAnalysis, get_crimeWeatherAnalysis)
        repo.logout()

        return doc'''

crimeWeatherTransformation.execute()
#doc = crimeWeatherTransformation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))