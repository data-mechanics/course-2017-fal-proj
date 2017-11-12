import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy

class stat_cor:
    contributor = 'gaudiosi_raykatz_nedg'
    reads = ['gaudiosi_raykatz_nedg.zipcode_info']
    writes = ['gaudiosi_raykatz_nedg.stat_cor']

    @staticmethod
    def execute(trial = False):
        '''Merge zipcode info'''
        startTime = datetime.datetime.now()

        # Set up the database connection.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')

        zipcode_data = list(repo.gaudiosi_raykatz_nedg.zipcode_map.find({}))[0]

        zipcode_list = []
        for feature in zipcode_data["features"]:
            zipcode_list.append(feature['properties']['ZIP5'])

        zipcode_list = list(set(zipcode_list))

        median_income = []
        percent_transit = []
        median_rent = []
        percent_homes_occupied = []
        percent_homes_before_1939 = []
        percent_white = []
        percent_black = []
        percent_hispanic = []
        percent_asian = []
        data = []

        for zipcode in zipcode_list:
            z = {}
            z["zipcode"] = zipcode

            if(not (len(list(repo.gaudiosi_raykatz_nedg.zipcode_info.find({'zipcode': zipcode}))) == 0)):
                data.append(list(repo.gaudiosi_raykatz_nedg.zipcode_info.find({'zipcode': zipcode}))[0])
        for i in range(len(data)):
            median_income.append(float(data[i]['median_income']))
            percent_transit.append(float(data[i]['percent_public_transit']))
            median_rent.append(float(data[i]['median_rent']))
            percent_homes_occupied.append(float(data[i]['percent_homes_occupied']))
            percent_homes_before_1939.append(float(data[i]['percent_homes_built_before_1939']))
            percent_white.append(float(data[i]['percent_white']))
            percent_black.append(float(data[i]['percent_black']))
            percent_hispanic.append(float(data[i]['percent_hispanic']))
            percent_asian.append(float(data[i]['percent_asian']))
        corDict = {}
        corDict["Median income/transit"] = numpy.corrcoef(median_income, percent_transit)[0, 1]
        corDict["Median income/median rent"] =  numpy.corrcoef(median_income, median_rent)[0, 1]
        corDict["Median income/percent homes occupied"] =  numpy.corrcoef(median_income, percent_homes_occupied)[0, 1]
        corDict["Median income/percent homes built before 1930"] = numpy.corrcoef(median_income, percent_homes_before_1939)[0, 1]
        corDict["Median income/percent white"] =  numpy.corrcoef(median_income, percent_white)[0, 1]
        corDict["Median income/percent black"] = numpy.corrcoef(median_income, percent_black)[0, 1]
        corDict["Median income/percent hispanic"] =  numpy.corrcoef(median_income, percent_hispanic)[0, 1]
        corDict["Median income/percent asian"] =  numpy.corrcoef(median_income, percent_asian)[0, 1]

        repo.dropCollection("stat_cor")
        repo.createCollection("stat_cor")
        repo['gaudiosi_raykatz_nedg.stat_cor'].insert_one(corDict)
        repo['gaudiosi_raykatz_nedg.stat_cor'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz_nedg.stat_cor'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz_nedg', 'gaudiosi_raykatz_nedg')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:gaudiosi_raykatz_nedg#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:gaudiosi_raykatz_nedg#zipcode_info', {'prov:label':'Zipcode Info', prov.model.PROV_TYPE:'ont:DataSet'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)

        doc.usage(get_demos, resource, startTime, None,{prov.model.PROV_TYPE:'ont:computation'})

        demos = doc.entity('dat:gaudiosi_raykatz_nedg#stat_cor', {prov.model.PROV_LABEL:'Statistics Correlations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()

        return doc
