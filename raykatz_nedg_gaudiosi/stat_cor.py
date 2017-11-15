import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy

class stat_cor:
    contributor = 'raykatz_nedg_gaudiosi'
    reads = ['raykatz_nedg_gaudiosi.zipcode_info']
    writes = ['raykatz_nedg_gaudiosi.stat_cor']

    @staticmethod
    def execute(trial = False):
        '''Find correlations'''
        startTime = datetime.datetime.now()

        # Set up the database connection.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')

        zipcode_data = list(repo.raykatz_nedg_gaudiosi.zipcode_map.find({}))[0]

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
        percent_married = []
        percent_unemployed = []
        percent_50_rent = []
        percent_poverty = []
        bus_stops = []
        subway_stops = []
        data = []

        for zipcode in zipcode_list:
            if(not (len(list(repo.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode': zipcode}))) == 0)):
                data.append(list(repo.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode': zipcode}))[0])
        
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
            percent_married.append(float(data[i]['percent_married_households']))
            percent_unemployed.append(float(data[i]['percent_unemployed']))
            percent_50_rent.append(float(data[i]['percent_spending_50_rent']))
            percent_poverty.append(float(data[i]['percent_poverty']))
            bus_stops.append(float(data[i]['bus_stops']))
            subway_stops.append(float(data[i]['subway_stops']))
        
        corDict = {}
        corDict["Median income/median rent"] =  numpy.corrcoef(median_income, median_rent)[0, 1]
        corDict["Median income/percent taking public transit"] = numpy.corrcoef(median_income, percent_transit)[0, 1]
        corDict["Median income/unemployed"] = numpy.corrcoef(median_income,percent_unemployed)[0, 1]
        corDict["Median income/percent homes occupied"] =  numpy.corrcoef(median_income, percent_homes_occupied)[0, 1]
        corDict["Median income/percent homes built before 1939"] = numpy.corrcoef(median_income, percent_homes_before_1939)[0, 1]
        corDict["Median income/percent white"] =  numpy.corrcoef(median_income, percent_white)[0, 1]
        corDict["Median income/percent black"] = numpy.corrcoef(median_income, percent_black)[0, 1]
        corDict["Median income/percent hispanic"] =  numpy.corrcoef(median_income, percent_hispanic)[0, 1]
        corDict["Median income/percent asian"] =  numpy.corrcoef(median_income, percent_asian)[0, 1]
        corDict["Median income/percent married"] = numpy.corrcoef(median_income,percent_married)[0, 1]
        
        corDict["Median rent/percent taking public transit"] = numpy.corrcoef(median_rent, percent_transit)[0, 1]
        corDict["Median rent/unemployed"] = numpy.corrcoef(median_rent,percent_unemployed)[0, 1]
        corDict["Median rent/percent spending 50% income on rent"] = numpy.corrcoef(median_rent,percent_50_rent)[0, 1]
        corDict["Median rent/percent homes built before 1939"] = numpy.corrcoef(median_rent,percent_homes_before_1939)[0, 1]
        corDict["Median rent/poverty rate"] = numpy.corrcoef(median_rent,percent_poverty)[0, 1]
        corDict["Median rent/bus stops"] = numpy.corrcoef(median_rent,bus_stops)[0, 1]
        corDict["Median rent/subway stops"] = numpy.corrcoef(median_rent,subway_stops)[0, 1]
        corDict["Median rent/percent married"] = numpy.corrcoef(median_rent,percent_married)[0, 1]



        repo.dropCollection("stat_cor")
        repo.createCollection("stat_cor")
        repo['raykatz_nedg_gaudiosi.stat_cor'].insert_one(corDict)
        repo['raykatz_nedg_gaudiosi.stat_cor'].metadata({'complete':True})
        print(repo['raykatz_nedg_gaudiosi.stat_cor'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('raykatz_nedg_gaudiosi', 'raykatz_nedg_gaudiosi')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:raykatz_nedg_gaudiosi#proj2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:raykatz_nedg_gaudiosi#zipcode_info', {'prov:label':'Zipcode Info', prov.model.PROV_TYPE:'ont:DataSet'})
        get_demos = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_demos, this_script)

        doc.usage(get_demos, resource, startTime, None,{prov.model.PROV_TYPE:'ont:computation'})

        demos = doc.entity('dat:raykatz_nedg_gaudiosi#stat_cor', {prov.model.PROV_LABEL:'Statistics Correlations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(demos, this_script)
        doc.wasGeneratedBy(demos, get_demos, endTime)
        doc.wasDerivedFrom(demos, resource, get_demos, get_demos, get_demos)

        repo.logout()

        return doc
