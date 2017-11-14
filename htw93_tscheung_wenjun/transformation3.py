import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty
import scipy.stats

class transformation3(dml.Algorithm):
    contributor = 'htw93_tscheung_wenjun'
    reads = ['htw93_tscheung_wenjun.BostonHotelCustomScore']
    writes = ['htw93_tscheung_wenjun.BostonHotelCorrelation']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')

        BostonHotelCustomScore = repo.htw93_tscheung_wenjun.BostonHotelCustomScore
        #BostonHotelRatingOriginal = repo.htw93_tscheung_wenjun.BostonHotel
        #HotelRaing = BostonHotelRatingOriginal.find()
        CustomScoreArr = BostonHotelCustomScore.find()

        norm_rate = []
        norm_crime = []
        norm_mbta =[]
        norm_garden =[]
        norm_food =[]
        orginal_rate=[]
        # Combine boston crime and boston schools.
        res = []

        combine_rate_crime=[]
        
    
        for h in CustomScoreArr:
            print(h)
            orginal_rate.append([h['orginal_rate']])
            norm_rate.append([h['norm_rate']])
            norm_crime.append([h['norm_crime']])
            norm_mbta.append([h['norm_mbta']])
            norm_garden.append([h['norm_garden']])
            norm_food.append([h['norm_food']])

            combine_rate_crime.append([(h['norm_rate']+h['norm_crime']+h['norm_mbta']+h['norm_garden']+h['norm_food'])/5])
            #CustomScore.append([h['norm_custom_score']])


        # Trial mode: randomly choose k elements from lists
        if trial:
            orginal_rate = random.choices(orginal_rate, k = 1)
            norm_rate = random.choices(norm_rate, k = 1)
            norm_crime = random.choices(norm_crime, k = 1)
            norm_mbta = random.choices(norm_mbta, k = 1)
            norm_garden = random.choices(norm_garden, k = 1)
            norm_food = random.choices(norm_food, k = 1)
            combine_rate_crime = random.choices(combine_rate_crime, k = 1)
            


        math_score_crime = scipy.stats.pearsonr(combine_rate_crime, norm_crime)
        math_score_mbta = scipy.stats.pearsonr(combine_rate_crime, norm_mbta)
        math_score_garden = scipy.stats.pearsonr(combine_rate_crime, norm_garden)
        math_score_food = scipy.stats.pearsonr(combine_rate_crime, norm_food)
        '''
        math_score_crime = scipy.stats.pearsonr(norm_rate, norm_crime)
        math_score_mbta = scipy.stats.pearsonr(norm_rate, norm_mbta)
        math_score_garden = scipy.stats.pearsonr(norm_rate, norm_garden)
        math_score_food = scipy.stats.pearsonr(norm_rate, norm_food)
        '''
        print("Crime Correlation coefficient is " + str(math_score_crime[0]))
        print("Crime P-value is " + str(math_score_crime[1]))
        print("mbta Correlation coefficient is " + str(math_score_mbta[0]))
        print("mbta P-value is " + str(math_score_mbta[1]))
        print("Garden Correlation coefficient is " + str(math_score_garden[0]))
        print("Garden P-value is " + str(math_score_garden[1]))
        print("Food Correlation coefficient is " + str(math_score_food[0]))
        print("Fodd P-value is " + str(math_score_food[1]))        
        res.append({'crime_coefficient':math_score_crime[0][0], 'crime_p_value':math_score_crime[1][0]})
        res.append({'mbta_coefficient':math_score_mbta[0][0], 'mbta_p_value':math_score_mbta[1][0]})
        res.append({'garden_coefficient':math_score_garden[0][0], 'garden_p_value':math_score_garden[1][0]})
        res.append({'food_coefficient':math_score_food[0][0], 'food_p_value':math_score_food[1][0]})
        repo.dropCollection("BostonHotelCorrelation")
        repo.createCollection("BostonHotelCorrelation")
        repo['htw93_tscheung_wenjun.BostonHotelCorrelation'].insert_many(res)
        print('Finished creating collection htw93_tscheung_wenjun.BostonHotelCorrelation')
        
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
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        HotelCorrelation_script = doc.agent('alg:htw93_tscheung_wenjun#HotelCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_BostonHotelCustomScore = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelCustomScore', {'prov:label': 'BostonHotelCustomScore', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        # define activity to represent invocaton of the script
        run_HotelCorrelation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # associate the activity with the script
        doc.wasAssociatedWith(run_HotelCorrelation, HotelCorrelation_script)
        # indicate that an activity used the entity
        doc.usage(run_HotelCorrelation, resource_BostonHotelCustomScore, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        obtained_Correlation = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelCorrelation', {prov.model.PROV_LABEL: 'BostonHotelCorrelation', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(obtained_Correlation, HotelCorrelation_script)
        doc.wasGeneratedBy(obtained_Correlation, resource_BostonHotelCustomScore, endTime)
        doc.wasDerivedFrom(obtained_Correlation, resource_BostonHotelCustomScore, run_HotelCorrelation, run_HotelCorrelation, run_HotelCorrelation)
        

        repo.logout()
                  
        return doc

transformation3.execute()
doc = transformation3.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
