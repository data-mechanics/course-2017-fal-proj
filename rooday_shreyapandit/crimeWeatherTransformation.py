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
    def aggregate(R,f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    def project(R, p):
        return [p(t) for t in R]
    def removeDuplicates(seq):
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

    def crimeDateAndType(t):
        return (datetime.datetime.strptime(t['OCCURRED_ON_DATE'], '%Y-%m-%dT%H:%M:%S').date(), t['OFFENSE_DESCRIPTION'])

    def weatherDateAndType(t):
        return (datetime.datetime.strptime(t['BEGIN_DATE'], '%m/%d/%Y').date(), t['EVENT_TYPE'])

    def findWeatherType(date, weatherData):
        for weatherDate, weatherType in weatherData:
            if (date == weatherDate):
                return weatherType
        return 'None'

    contributor = 'rooday_shreyapandit'
    reads = ['rooday_shreyapandit.crime',
              'rooday_shreyapandit.weather']
    writes = ['rooday_shreyapandit.crimesByDateAndWeather']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        crimeData = repo['rooday_shreyapandit.crime']
        weatherData = repo['rooday_shreyapandit.weather']

        print("Filtering for crimes in 2016...")
        crimes2016 = crimeWeatherTransformation.select(crimeData.find(), crimeWeatherTransformation.betweenDates)
        #stormsAndCrimes = crimeWeatherTransformation.select(crimeWeatherTransformation.product(weatherData.find(), crimeData.find()), crimeWeatherTransformation.equalDates)

        print("Aggregating total crimes by date...")
        crimesByDate = crimeWeatherTransformation.aggregate(crimeWeatherTransformation.project(crimes2016, crimeWeatherTransformation.crimeDateAndType), len)
        print("Projecting weather data for date and type...")
        weatherDatesAndTypes = crimeWeatherTransformation.project(weatherData.find(), crimeWeatherTransformation.weatherDateAndType)

        print("Generating final list of dates, crime totals, and weather types...")
        finalList = []
        for crimeDate, crimeNum in crimesByDate:
            finalList.append({'date': str(crimeDate), 'crimeNum': crimeNum, 'weatherType': crimeWeatherTransformation.findWeatherType(crimeDate, weatherDatesAndTypes)})

        print("Sorting final list...")
        finalList.sort(key=lambda x: x['date'])

        print('DONE!')
        repo.dropCollection('crimesByDateAndWeather')
        repo.createCollection('crimesByDateAndWeather')
        repo['rooday_shreyapandit.crimesByDateAndWeather'].insert_many(finalList)
        
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