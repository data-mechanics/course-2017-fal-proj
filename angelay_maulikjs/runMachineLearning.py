import csv
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from sklearn.neural_network import MLPRegressor
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import r2_score
import pandas as pd
import numpy as np
import math as math
from sklearn.preprocessing import StandardScaler  
import matplotlib.pyplot as plt



class runMachineLearning(dml.Algorithm):
    contributor = 'angelay_maulikjs'
    reads = ['angelay_maulikjs.clean2012', 'angelay_maulikjs.clean2013']
    writes = []

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        repo.dropPermanent('angelay_maulikjs')
        repo.createPermanent('angelay_maulikjs')

        data= repo.angelay_maulikjs.clean2012.find()

        y=[]
        x=[]

        for document in data:
            d = dict(document)
            tmp=[]
            tmp.append(d['CarbonIntensity'])
            tmp.append(d['EnergyIntensity'])
            tmp.append(d['EnergyUse'])
            tmp.append(d['GDPperCapita'])
            tmp.append(d['HDI'])
            tmp.append(d['Population'])
            y.append(d['CO2Emissions'])
            x.append(tmp)

        data2013= repo.angelay_maulikjs.clean2013.find()

        y13=[]
        x13=[]

        for document in data2013:
            d = dict(document)
            tmp=[]
            tmp.append(d['CarbonIntensity'])
            tmp.append(d['EnergyIntensity'])
            tmp.append(d['EnergyUse'])
            tmp.append(d['GDPperCapita'])
            tmp.append(d['HDI'])
            tmp.append(d['Population'])
            y13.append(d['CO2Emissions'])
            x13.append(tmp)
        

        scaler = StandardScaler()  
        scaler.fit(x)  
        X_train = scaler.transform(x)  

        # scaler13 = StandardScaler()  
        # scaler13.fit(x13)  
        X_train13 = scaler.transform(x13) 

        
        # df.to_csv('test.csv', index=False, header=False)
        # clf = 0

        clf=MLPRegressor(hidden_layer_sizes=(7776,), max_iter=1000 ,learning_rate_init=0.001,momentum=0.4,alpha=0.01)
        neural_model = clf.fit(X_train,y)
        validation_data_predictions = clf.predict(X_train13)
        r2_error = r2_score(y_true=y13, y_pred=validation_data_predictions)


        fig, ax = plt.subplots()
        x1 = range(len(validation_data_predictions))
        ax.plot(x1, y13, 'o', label="Actual Data (2013)")
        ax.plot(x1, validation_data_predictions, 'r', label="Multilayer Perceptron Predicted Data (2013)")
        ax.legend(loc="best")
        
        plt.savefig('angelay_maulikjs/MLP.png', bbox_inches='tight')
 
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ang', 'http://datamechanics.io/data/angelay_maulikjs/')

        this_script = doc.agent('dat:angelay#runMachineLearning', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        MLmodel = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Build a Machine Learning( Multilayer Perceptron Neural Network) Model to Predict CO2 Emissions', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(MLmodel, this_script)
        
        resource_clean2012 = doc.entity('dat:angelay#clean2012', {'prov:label':'All Data from 2012 with Outliers Removed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(MLmodel, resource_clean2012, startTime)
        
        resource_clean2013 = doc.entity('dat:angelay#clean2013', {'prov:label':'All Data from 2013 with Outliers Removed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(MLmodel, resource_clean2013, startTime)

        repo.logout()

        return doc

# runMachineLearning.execute()
