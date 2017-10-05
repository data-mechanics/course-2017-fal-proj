import urllib.request
import json
import dml, prov.model
import datetime, uuid


"""
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

Local Climate Data from NOAA -- Boston, MA -- 9/1/2008 through 9/1/2017

Development notes:

-Grossly inefficient in how it adds Long/Lat/station/elevation to every entry. Also, 
a lot of the fields are left blank. Leaving it the way it is, though, for anyone else
who would want to use this script and would want that data in there.
"""


class retrieve_boston_LCD(dml.Algorithm):
	contributor = 'bmroach'
	reads = []
	writes = ['bmroach.boston_LCD']
	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('bmroach', 'bmroach')

		# Do retrieving of data    
		url = 'http://datamechanics.io/data/bmroach/boston_climate_data.csv'
		response = urllib.request.urlopen(url).read().decode("utf-8")
		repo.dropCollection("boston_LCD")
		repo.createCollection("boston_LCD")
		rowCount = 0
		lcdList = []
		fieldIndices = {
					0: 'STATION',
					1: 'STATION_NAME',
					2: 'ELEVATION',
					3: 'LATITUDE',
					4: 'LONGITUDE',
					5: 'DATE',
					6: 'REPORTTPYE',
					7: 'HOURLYSKYCONDITIONS',
					8: 'HOURLYVISIBILITY',
					9: 'HOURLYPRSENTWEATHERTYPE',
					10: 'HOURLYDRYBULBTEMPF',
					11: 'HOURLYDRYBULBTEMPC',
					12: 'HOURLYWETBULBTEMPF',
					13: 'HOURLYWETBULBTEMPC',
					14: 'HOURLYDewPointTempF',
					15: 'HOURLYDewPointTempC',
					16: 'HOURLYRelativeHumidity',
					17: 'HOURLYWindSpeed',
					18: 'HOURLYWindDirection',
					19: 'HOURLYWindGustSpeed',
					20: 'HOURLYStationPressure',
					21: 'HOURLYPressureTendency',
					22: 'HOURLYPressureChange',
					23: 'HOURLYSeaLevelPressure',
					24: 'HOURLYPrecip',
					25: 'HOURLYAltimeterSetting',
					26: 'DAILYMaximumDryBulbTemp',
					27: 'DAILYMinimumDryBulbTemp',
					28: 'DAILYAverageDryBulbTemp',
					29: 'DAILYDeptFromNormalAverageTemp',
					30: 'DAILYAverageRelativeHumidity',
					31: 'DAILYAverageDewPointTemp',
					32: 'DAILYAverageWetBulbTemp',
					33: 'DAILYHeatingDegreeDays',
					34: 'DAILYCoolingDegreeDays',
					35: 'DAILYSunrise',
					36: 'DAILYSunset',
					37: 'DAILYWeather',
					38: 'DAILYPrecip',
					39: 'DAILYSnowfall',
					40: 'DAILYSnowDepth',
					41: 'DAILYAverageStationPressure',
					42: 'DAILYAverageSeaLevelPressure',
					43: 'DAILYAverageWindSpeed',
					44: 'DAILYPeakWindSpeed',
					45: 'PeakWindDirection',
					46: 'DAILYSustainedWindSpeed',
					47: 'DAILYSustainedWindDirection',
					48: 'MonthlyMaximumTemp',
					49: 'MonthlyMinimumTemp',
					50: 'MonthlyMeanTemp',
					51: 'MonthlyAverageRH',
					52: 'MonthlyDewpointTemp',
					53: 'MonthlyWetBulbTemp',
					54: 'MonthlyAvgHeatingDegreeDays',
					55: 'MonthlyAvgCoolingDegreeDays',
					56: 'MonthlyStationPressure',
					57: 'MonthlySeaLevelPressure',
					58: 'MonthlyAverageWindSpeed',
					59: 'MonthlyTotalSnowfall',
					60: 'MonthlyDeptFromNormalMaximumTemp',
					61: 'MonthlyDeptFromNormalMinimumTemp',
					62: 'MonthlyDeptFromNormalAverageTemp',
					63: 'MonthlyDeptFromNormalPrecip',
					64: 'MonthlyTotalLiquidPrecip',
					65: 'MonthlyGreatestPrecip',
					66: 'MonthlyGreatestPrecipDate',
					67: 'MonthlyGreatestSnowfall',
					68: 'MonthlyGreatestSnowfallDate',
					69: 'MonthlyGreatestSnowDepth',
					70: 'MonthlyGreatestSnowDepthDate',
					71: 'MonthlyDaysWithGT90Temp',
					72: 'MonthlyDaysWithLT32Temp',
					73: 'MonthlyDaysWithGT32Temp',
					74: 'MonthlyDaysWithLT0Temp',
					75: 'MonthlyDaysWithGT001Precip',
					76: 'MonthlyDaysWithGT010Precip',
					77: 'MonthlyDaysWithGT1Snow',
					78: 'MonthlyMaxSeaLevelPressureValue',
					79: 'MonthlyMaxSeaLevelPressureDate',
					80: 'MonthlyMaxSeaLevelPressureTime',
					81: 'MonthlyMinSeaLevelPressureValue',
					82: 'MonthlyMinSeaLevelPressureDate',
					83: 'MonthlyMinSeaLevelPressureTime',
					84: 'MonthlyTotalHeatingDegreeDays',
					85: 'MonthlyTotalCoolingDegreeDays',
					86: 'MonthlyDeptFromNormalHeatingDD',
					87: 'MonthlyDeptFromNormalCoolingDD',
					88: 'MonthlyTotalSeasonToDateHeatingDD',
					89: 'MonthlyTotalSeasonToDateCoolingDD'
					}

		for line in response.split('\n')[1:]:
			lineDict = {}
			if line == '':
				continue
			line = line.split(',')
			for index, fieldName in fieldIndices.items():
				lineDict[fieldName] = line[index]

			lcdList.append( {str(rowCount) : lineDict} )   
			rowCount += 1
        

		repo['bmroach.boston_LCD'].insert_many( lcdList )

		repo['bmroach.boston_LCD'].metadata({'complete':True})  
		repo.logout()
		endTime = datetime.datetime.now()
		print("Boston Local Climate Data successfully added to database")
		return {"start":startTime, "end":endTime}
    
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
			Create the provenance document describing everything happening
			in this script. Each run of the script will generate a new
			document describing that invocation event.
			'''

        
        
                  
		return 





retrieve_boston_LCD.execute()

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
