import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
import geojson
from vincenty import vincenty

class transformation2(dml.Algorithm):
    contributor = 'htw93_tscheung_wenjun'
    reads = ['htw93_tscheung_wenjun.BostonHotelData']
    writes = ['htw93_tscheung_wenjun.BostonHotelCustomScore']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('htw93_tscheung_wenjun', 'htw93_tscheung_wenjun')

        BostonHotelData = repo.htw93_tscheung_wenjun.BostonHotelData
        BostonHotelRatingOriginal = repo.htw93_tscheung_wenjun.BostonHotel
        HotelRaing = BostonHotelRatingOriginal.find()
        HotelData = BostonHotelData.find()

        CustomScore = []
        
        # Combine boston crime and boston schools.
        max_crime=HotelData[0]['crime_count']
        max_mbta=HotelData[0]['mbta_count']
        min_crime=HotelData[0]['crime_count']
        min_mbta=HotelData[0]['mbta_count']
        max_rating = HotelData[0]['rate']
        min_rating = HotelData[0]['rate']
        max_food = HotelData[0]['food_count']
        min_food = HotelData[0]['food_count']
        max_garden = HotelData[0]['garden_count']
        min_garden = HotelData[0]['garden_count']
        for h in HotelData:
            crime_count = h['crime_count']
            mbta_count = h['mbta_count']
            rating = h['rate']
            food = h['food_count']
            garden  = h['garden_count']
            max_crime = max(max_crime,crime_count)
            max_mbta = max(max_mbta,mbta_count)
            max_rating = max(max_rating, rating)
            max_food = max(max_food, food)
            max_garden = max(max_garden, garden)
            min_crime = min(min_crime, crime_count)
            min_mbta = min(min_mbta, mbta_count)
            min_rating = min(min_rating, rating)
            min_food = min(min_food, food)
            min_garden = min(min_garden,garden)
            
            
            
        HotelData.rewind()

        

        for h in HotelData:
            norm_crime = 1-((h['crime_count']-min_crime)/(max_crime-min_crime))
            norm_mbta = (h['mbta_count']-min_mbta)/(max_mbta-min_mbta)
            norm_score = (h['rate']-min_rating)/(max_rating-min_rating)
            norm_garden =(h['garden_count']-min_garden)/(max_garden-min_garden)
            norm_food =(h['food_count']-min_food)/(max_food-min_food)
            
           # norm_custom_score = (norm_crime+norm_mbta+norm_score+norm_garden+norm_food)/5
            
            #CustomScore.append({'hotel':h['hotel'],'norm_rate': norm_score, 'norm_custom_score': norm_custom_score})
            #print(score)
            CustomScore.append({'hotel':h['hotel'],'orginal_rate':h['rate'],'norm_rate': norm_score, 'norm_crime':norm_crime, 'norm_mbta': norm_mbta, 'norm_garden': norm_garden, 'norm_food':norm_food})
            

        
            
        #print(CustomScore)
        repo.dropCollection("BostonHotelCustomScore")
        repo.createCollection("BostonHotelCustomScore")
        repo['htw93_tscheung_wenjun.BostonHotelCustomScore'].insert_many(CustomScore)
        print('Finished creating collection htw93_tscheung_wenjun.BostonHotelCustomScore')
        
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
        
        CustomScore_script = doc.agent('alg:htw93_tscheung_wenjun#transformationCustomScore', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_BostonHotelData = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelData', {'prov:label': 'BostonHotelData', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        # define activity to represent invocaton of the script
        run_transformationCustomScore = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        # associate the activity with the script
        doc.wasAssociatedWith(run_transformationCustomScore, CustomScore_script)
        # indicate that an activity used the entity
        doc.usage(run_transformationCustomScore, resource_BostonHotelData, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        obtained_CustomScore = doc.entity('dat:htw93_tscheung_wenjun#BostonHotelCustomScore', {prov.model.PROV_LABEL: 'BostonHotelCustomScore', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(obtained_CustomScore, CustomScore_script)
        doc.wasGeneratedBy(obtained_CustomScore, run_transformationCustomScore, endTime)
        doc.wasDerivedFrom(obtained_CustomScore, resource_BostonHotelData, run_transformationCustomScore, run_transformationCustomScore, run_transformationCustomScore)
        

        repo.logout()
                  
        return doc

#transformation2.execute(True)
#doc = transformation2.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
