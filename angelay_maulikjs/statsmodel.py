import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from scipy.stats.stats import pearsonr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import statsmodels.formula.api as smf
from sklearn.metrics import r2_score

class statsmodel(dml.Algorithm):
    contributor = 'angelay_maulikjs'
    reads = ['angelay_maulikjs.clean2012', 'angelay_maulikjs.all2012', 'angelay_maulikjs.clean2013', 'angelay_maulikjs.all2013']
    writes = []

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('angelay_maulikjs', 'angelay_maulikjs')

        repo.dropPermanent('angelay_maulikjs')
        repo.createPermanent('angelay_maulikjs')
        
        data = repo.angelay_maulikjs.clean2012.find()
        D = []
        for document in data:
            d = dict(document)
            D.append([d['CarbonIntensity'], d['CO2Emissions'], d['EnergyIntensity'], d['EnergyUse'], d['GDPperCapita'], d['HDI'], d['Population']])
        df = pd.DataFrame(D, columns = ['CarbonIntensity', 'CO2Emissions', 'EnergyIntensity', 'EnergyUse', 'GDPperCapita', 'HDI', 'Population'])
        
        axes = pd.plotting.scatter_matrix(df, alpha=1, figsize=(10, 10))
        plt.tight_layout()
        plt.savefig('angelay_maulikjs/statsmodel_correlations')
        print('\nCorrelation coefficients:\n')
        print(df.corr())
        print()
        print(df.describe())
        print()
        
        Independents = df[['CarbonIntensity','EnergyIntensity','EnergyUse','GDPperCapita', 'HDI','Population']]
        Dependent = df.CO2Emissions
        model = sm.OLS(Dependent, Independents)
        results = model.fit()
        print(results.summary())
        print()
        print('\nThis is our linear least-square model. It does not yield a good R-squared value. We are going to build our model by adding one variable at a time, starting with the variable that yields the highest R-squared value when fitted to a linear model against CO2 emissions. After doing some research, we found a theory called Kaya Identity which states that CO2 emissions is roughly equal to population * GDP per capita * energy intensity * carbon intensity. We will incorporate that into our model and see if adding energy use and HDI will make it better.\n')
        ind = ['CarbonIntensity','EnergyIntensity','EnergyUse','GDPperCapita', 'HDI','Population']
        for i in range(len(ind)):
            model = sm.OLS(Dependent, df[ind[i]])
            results = model.fit()
            print('\nCO2Emissions vs ' + ind[i] + '\n')
            print(results.summary())
        print('\nLooks like population yields the highest R-squared value, and energy use comes next. Build a model with population and energy use and see if R-squared value goes up.\n')
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse', data=df)
        results = model.fit()
        print('\nCO2Emissions vs Population + EnergyUse\n')
        print(results.summary())
        print('\nR-squared value went up to 0.954. Now adding Carbon Intensity.\n')
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse * CarbonIntensity', data=df)
        results = model.fit()
        print('\nCO2Emissions vs Population * EnergyUse * CarbonIntensity\n')
        print(results.summary())
        print('\nR-squared value went up to 0.982. Now adding GDP per capita.\n')
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse * CarbonIntensity * GDPperCapita', data=df)
        results = model.fit()
        print('\nCO2Emissions vs Population * EnergyUse * CarbonIntensity * GDPperCapita\n')
        print(results.summary())
        print('\nR-squared value went up to 0.992. Now adding HDI.\n')
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI', data=df)
        results = model.fit()
        print('\nCO2Emissions vs Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI\n')
        print(results.summary())
        print('\nR-squared value went up to 0.995. Now adding Energy Intensity.\n')
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI * EnergyIntensity', data=df)
        results = model.fit()
        print('\nCO2Emissions vs Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI * EnergyIntensity\n')
        print(results.summary())
        #print(results.params)
        print('\nR-squared value went up to 0.998. This is a really high R-squared value. We might be at risk of overfitting the data. Lets test our model on the 2013 data and see how we do.\n')
        
        # getting the 2013 data
        data2 = repo.angelay_maulikjs.clean2013.find()
        D2 = []
        for document in data2:
            d = dict(document)
            D2.append([d['CarbonIntensity'], d['CO2Emissions'], d['EnergyIntensity'], d['EnergyUse'], d['GDPperCapita'], d['HDI'], d['Population']])
        df2 = pd.DataFrame(D2, columns = ['CarbonIntensity', 'CO2Emissions', 'EnergyIntensity', 'EnergyUse', 'GDPperCapita', 'HDI', 'Population'])
        # Testing the model on 2013 data
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI * EnergyIntensity', data=df)
        pred = model.fit().predict(df2)
        R2 = r2_score(df2.CO2Emissions, pred)
        print('\nResults on 2013 Data\n')
        fig, ax = plt.subplots()
        x2 = range(len(df2.index))
        ax.plot(x2, df2.CO2Emissions, 'o', label="Data")
        ax.plot(x2, pred, 'r', label="OLS prediction")
        ax.legend(loc="best")
        plt.savefig('angelay_maulikjs/statsmodel_results_without_outliers')

        print('\nThe R-squared value is %f, this is a pretty good R-squared value, and it means that our model does pretty well at predicting future values. Now lets train the model on all 2012 data with ourliers and test it on all 2013 data with outliers.\n' % R2)
        
        # getting all 2012 data with outliers
        data3 = repo.angelay_maulikjs.all2012.find()
        D3 = []
        for document in data3:
            d = dict(document)
            D3.append([d['CarbonIntensity'], d['CO2Emissions'], d['EnergyIntensity'], d['EnergyUse'], d['GDPperCapita'], d['HDI'], d['Population']])
        df3 = pd.DataFrame(D3, columns = ['CarbonIntensity', 'CO2Emissions', 'EnergyIntensity', 'EnergyUse', 'GDPperCapita', 'HDI', 'Population'])
        # Training the model on all 2012 data
        model = smf.ols(formula='CO2Emissions ~ Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI * EnergyIntensity', data=df3)
        results = model.fit()
        print('\nCO2Emissions vs Population * EnergyUse * CarbonIntensity * GDPperCapita * HDI * EnergyIntensity\n')
        print(results.summary())
        #print(results.params)
        
        print('\nWe got an R-squared value of 1, but we might be overfitting the data. Lets test the model on all 2013 data to see how we do.\n')
        
        # getting all 2013 data with outliers
        data4 = repo.angelay_maulikjs.all2013.find()
        D4 = []
        for document in data4:
            d = dict(document)
            D4.append([d['CarbonIntensity'], d['CO2Emissions'], d['EnergyIntensity'], d['EnergyUse'], d['GDPperCapita'], d['HDI'], d['Population']])
        df4 = pd.DataFrame(D4, columns = ['CarbonIntensity', 'CO2Emissions', 'EnergyIntensity', 'EnergyUse', 'GDPperCapita', 'HDI', 'Population'])
        # Testing the model on all 2013 data
        pred = model.fit().predict(df4)
        R2 = r2_score(df4.CO2Emissions, pred)
        print('\nResults on All 2013 Data\n')
        fig, ax = plt.subplots()
        x1 = range(len(df4.index))
        ax.plot(x1, df4.CO2Emissions, 'o', label="Data")
        ax.plot(x1, pred, 'r', label="OLS prediction")
        ax.legend(loc="best")
        plt.savefig('angelay_maulikjs/statsmodel_results_with_outliers')
        
        print('\nThe R-squared value is %f, even higher than the one we got from clean data without outliers. We can be pretty confident about our model being able to predict future values now.\n' % R2)
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

        
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

        this_script = doc.agent('dat:angelay#statsmodel', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        statsmodel = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Build a Statistic Model to Predict CO2 Emissions', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(statsmodel, this_script)
        
        resource_clean2012 = doc.entity('dat:angelay#clean2012', {'prov:label':'All Data from 2012 with Outliers Removed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(statsmodel, resource_clean2012, startTime)

        resource_all2012 = doc.entity('dat:angelay#all2012', {'prov:label':'All Data from 2012', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(statsmodel, resource_all2012, startTime)
        
        resource_clean2013 = doc.entity('dat:angelay#clean2013', {'prov:label':'All Data from 2013 with Outliers Removed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(statsmodel, resource_clean2013, startTime)
        
        resource_all2013 = doc.entity('dat:angelay#all2013', {'prov:label':'All Data from 2013', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(statsmodel, resource_all2013, startTime)

        repo.logout()

        return doc

#statsmodel.execute()
