import urllib.request
from bson import json_util
import dml
import prov.model
import datetime
import uuid



class library_rain_correlation(dml.Algorithm):
    contributor = 'sbrz_nedg'
    reads = ['sbrz_nedg.libraryData', 'sbrz_nedg.rainData']
    writes = ['sbrz_nedg.union_rain_library']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')

        #getting data that contains average rainfall of every month
        rainDataCursor = repo.sbrz_nedg.rainData
        rainData = [x for x in rainDataCursor.find()]

        #getting library visit data
        libDataCursor = repo.sbrz_nedg.libraryData
        days = [x for x in libDataCursor.find()]
        days = days[0]['result']['records'] + days[1]['result']['records'] + days[2]['result']['records'] + days[3]['result']['records'] + days[4]['result']['records']



        janData = []
        febData = []
        marData = []
        aprData = []
        mayData = []
        junData = []
        julData = []
        augData = []
        sepData = []
        octData = []
        novData = []
        decData = []

        for item in days:
            if(item['Date'][:2] == '01'):
                janData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '02'):
                febData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '03'):
                marData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '04'):
                aprData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '05'):
                mayData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '06'):
                junData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '07'):
                julData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '08'):
                augData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '09'):
                sepData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '10'):
                octData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '11'):
                novData.append(int(item["Active Library Users"]))
            if(item['Date'][:2] == '12'):
                decData.append(int(item["Active Library Users"]))


        janAvg = str(sum(janData)/len(janData))
        febAvg = str(sum(febData)/len(febData))
        marAvg = str(sum(marData)/len(marData))
        aprAvg = str(sum(aprData)/len(aprData))
        mayAvg = str(sum(mayData)/len(mayData))
        junAvg = str(sum(junData)/len(junData))
        julAvg = str(sum(julData)/len(julData))
        augAvg = str(sum(augData)/len(augData))
        sepAvg = str(sum(sepData)/len(sepData))
        octAvg = str(sum(octData)/len(octData))
        novAvg = str(sum(novData)/len(novData))
        decAvg = str(sum(decData)/len(decData))

        rainDataJan = rainData[0]['trip']['precip']['avg']['cm']
        rainDataFeb = rainData[1]['trip']['precip']['avg']['cm']
        rainDataMar = rainData[2]['trip']['precip']['avg']['cm']
        rainDataApr = rainData[3]['trip']['precip']['avg']['cm']
        rainDataMay = rainData[4]['trip']['precip']['avg']['cm']
        rainDataJun = rainData[5]['trip']['precip']['avg']['cm']
        rainDataJul = rainData[6]['trip']['precip']['avg']['cm']
        rainDataAug = rainData[7]['trip']['precip']['avg']['cm']
        rainDataSep = rainData[8]['trip']['precip']['avg']['cm']
        rainDataOct = rainData[9]['trip']['precip']['avg']['cm']
        rainDataNov = rainData[10]['trip']['precip']['avg']['cm']
        rainDataDec = rainData[11]['trip']['precip']['avg']['cm']

        repo.dropCollection('sbrz_nedg.union_rain_library')
        repo.createCollection('sbrz_nedg.union_rain_library')


        january = {'month': 'jan', 'rain': rainDataJan, 'users': janAvg}
        february = {'month': 'feb', 'rain': rainDataFeb, 'users': febAvg}
        march = {'month': 'mar', 'rain': rainDataMar, 'users': marAvg}
        april = {'month': 'apr', 'rain': rainDataApr, 'users': aprAvg}
        may = {'month': 'may', 'rain': rainDataMay, 'users': mayAvg}
        june = {'month': 'jun', 'rain': rainDataJun, 'users': junAvg}
        july = {'month': 'jul', 'rain': rainDataJul, 'users': julAvg}
        august = {'month': 'aug', 'rain': rainDataAug, 'users': augAvg}
        september = {'month': 'sep', 'rain': rainDataSep, 'users': sepAvg}
        october = {'month': 'oct', 'rain': rainDataOct, 'users': octAvg}
        november = {'month': 'nov', 'rain': rainDataNov, 'users': novAvg}
        december = {'month': 'dec', 'rain': rainDataDec, 'users': decAvg}


        repo['sbrz_nedg.union_rain_library'].insert_one(january)
        repo['sbrz_nedg.union_rain_library'].insert_one(february)
        repo['sbrz_nedg.union_rain_library'].insert_one(march)
        repo['sbrz_nedg.union_rain_library'].insert_one(april)
        repo['sbrz_nedg.union_rain_library'].insert_one(may)
        repo['sbrz_nedg.union_rain_library'].insert_one(june)
        repo['sbrz_nedg.union_rain_library'].insert_one(july)
        repo['sbrz_nedg.union_rain_library'].insert_one(august)
        repo['sbrz_nedg.union_rain_library'].insert_one(september)
        repo['sbrz_nedg.union_rain_library'].insert_one(october)
        repo['sbrz_nedg.union_rain_library'].insert_one(november)
        repo['sbrz_nedg.union_rain_library'].insert_one(december)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('sbrz_nedg', 'sbrz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:sbrz_nedg#library_rain_correlation', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        visitRain_db = doc.entity({'prov:label': 'library_rain_correlation', prov.model.PROV_TYPE: 'ont:DataSet'})

        library_rain_correlation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_script)
        doc.usage(library_rain_correlation, visitRain_db, startTime)


        doc.wasAttributedTo(this_script)
        doc.wasGeneratedBy(library_rain_correlation)
        doc.wasDerivedFrom(visitRain_db)

        repo.logout()

        return doc
