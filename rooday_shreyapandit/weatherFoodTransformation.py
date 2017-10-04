import dml
import prov.model
import datetime
import uuid
import re
from itertools import chain

class weatherFoodTransformation(dml.Algorithm):

    '''
        Helper methods courtesy Professor Andrei Lapets lapets@bu.edu
    '''
    def union(R, S):
        return R + S

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    contributor = 'rooday_shreyapandit'
    reads = ['rooday_shreyapandit.weather', 'rooday_shreyapandit.foodviolations']
    writes = ['rooday_shreyapandit.weatherFoodCombo']

    @staticmethod
    def execute(trial=False):
        print("Starting...")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        weather_data = repo['rooday_shreyapandit.weather']
        food_data = repo['rooday_shreyapandit.foodviolations']
        weather_incidents_orig = []
        weather_incidents_prop = []

        food_incidents_orig = []
        foodincidents_by_month = []
        weatherincidents_by_month = []

        #We dont need everything, just extract month from date, and number of properties damaged
        for wrecord in weather_data.find():
            if 'BEGIN_DATE' in wrecord:
                month_year = wrecord['BEGIN_DATE'].split("/")[0] + wrecord['BEGIN_DATE'].split("/")[2]

                # Make dataset of form {month_year, occurences}
                weather_incidents_orig.append((month_year,1))
                weather_incidents_prop.append((month_year, int(wrecord['DAMAGE_PROPERTY_NUM'])))

        food_data_2016 = food_data.find({ 'VIOLDTTM': {'$regex': re.compile('2016', re.IGNORECASE)}})     
        food_data_2017 = food_data.find({ 'VIOLDTTM': {'$regex': re.compile('2017', re.IGNORECASE)}})
        total_food_data = [x for x in chain(food_data_2016, food_data_2017)]

        print("found records for latest food violations: " + str(len(total_food_data)))
 
        for frecord in total_food_data:
            if 'VIOLDTTM' in frecord:
                month_year = frecord['VIOLDTTM'].split("-")[1] + frecord['VIOLDTTM'].split("-")[0]
                food_incidents_orig.append((month_year,1))


        #Now aggregate by summing together incidents for each month
        weatherpropincidents_by_month = weatherFoodTransformation.aggregate(weather_incidents_prop, sum)
        weatheroccurenceincidents_by_month = weatherFoodTransformation.aggregate(weather_incidents_orig, sum)
        foodincidents_by_month = weatherFoodTransformation.aggregate(food_incidents_orig, sum)

        #Now food data is in form {month_year, num_violations} and we have a datset in the form {month_year, occurences of bad weather}
        #We now join these two datsets using the common key which is month_year

        final_combined_data = weatherFoodTransformation.project(weatherFoodTransformation.select(weatherFoodTransformation.product(foodincidents_by_month , weatheroccurenceincidents_by_month), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))

        print("****The food incidents by month********")
        print(foodincidents_by_month)
        print("****Bad weather occurences by month******")
        print(weatheroccurenceincidents_by_month)

        data_to_save = []
        for record in final_combined_data:
            data_to_save.append({'MonthYear': record[0], 'FoodViolatons': record[1],'NumBadWeatherDays': record[2] })

        print("final data set is:" +str(len(data_to_save)))
        print(data_to_save)

        repo.dropCollection('weatherFoodCombo')
        repo.createCollection('weatherFoodCombo')
        repo['rooday_shreyapandit.weatherFoodCombo'].insert_many(data_to_save)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        """
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rooday_shreyapandit', 'rooday_shreyapandit')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:rooday_shreyapandit#weatherFoodTransformation', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_weather = doc.entity('dat:rooday_shreyapandit#weather', {'prov:label': 'Inclement Weather Data for Boston and Suffolk', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_food = doc.entity('dat:rooday_shreyapandit#foodviolations', {'prov:label': 'Food Inspection Data for Boston', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_weatherFoodCombo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_weatherFoodCombo, this_script)

        doc.usage(get_weatherFoodCombo, resource_weather, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_weatherFoodCombo, resource_food, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        weather_food = doc.entity('dat:rooday_shreyapandit#weatherFoodCombo', {prov.model.PROV_LABEL: 'Food Weather Combined Dataset', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(weather_food, this_script)
        doc.wasGeneratedBy(weather_food, get_weatherFoodCombo, endTime)
        doc.wasDerivedFrom(weather_food, resource_weather, get_weatherFoodCombo, get_weatherFoodCombo, get_weatherFoodCombo)
        doc.wasDerivedFrom(weather_food, resource_food, get_weatherFoodCombo, get_weatherFoodCombo, get_weatherFoodCombo)

        repo.logout()

        return doc

# weatherFoodTransformation.execute()