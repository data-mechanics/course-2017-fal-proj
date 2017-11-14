import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import random

#calculations to get correlation for # crimes per street : # of properties per street
class calculateCorrelations(dml.Algorithm):
    contributor = 'bohorqux_peterg04_rocksdan_yfchen'
    reads = ['bohorqux_peterg04_rocksdan_yfchen.property_crimes']
    writes = ['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations']

    # Taking all the helper functions given in class by Professor Lapets
    def permute(x):
        shuffled = [xi for xi in x]
        random.shuffle(shuffled)
        return shuffled

    def avg(x): # Average
        return sum(x)/len(x)
    
    def stddev(x): # Standard deviation.
        m = calculateCorrelations.avg(x)
        return math.sqrt(sum([(xi-m)**2 for xi in x])/len(x))
    
    def cov(x, y): # Covariance.
        return sum([(xi-calculateCorrelations.avg(x))*(yi-calculateCorrelations.avg(y)) for (xi,yi) in zip(x,y)])/len(x)
    
    def corr(x, y): # Correlation coefficient.
        if calculateCorrelations.stddev(x)*calculateCorrelations.stddev(y) != 0:
            return calculateCorrelations.cov(x, y)/(calculateCorrelations.stddev(x)*calculateCorrelations.stddev(y))
     
    def p(x, y):
        c0 = calculateCorrelations.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = calculateCorrelations.permute(y)
            corrs.append(calculateCorrelations.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)
        
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        print("Creating calculateCorrelations...")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

        # code to calculate the correlation between crimes and property related values
        # grabbing all the x and y from first read file
        c_p = repo[calculateCorrelations.reads[0]].find()
        x_vals = []
        y_vals = []
        
        for key in c_p[0]:              
            if(key == '_id'):
                # do nothing
                continue
            else:
                # append to list of x-y lists 
                x_vals.append(c_p[0][key]['Crimes'])
                y_vals.append(len(c_p[0][key]['Properties']))
        
        # I want to find two seperate correlations related to this data. Whether crime is more correlated
        # with high density areas or lower density areas
        # Do this by splitting the values into 2 sets: 1 lower than avg , 1 higher than avg for # of properties
        prop_lower_x = []
        prop_lower_y = []
        prop_higher_x = []
        prop_higher_y = []
        avg_yvals = calculateCorrelations.avg(y_vals)
        for i in range(len(y_vals)):
            if y_vals[i] < avg_yvals:
                prop_lower_x.append(x_vals[i])
                prop_lower_y.append(y_vals[i])
            else:
                prop_higher_x.append(x_vals[i])
                prop_higher_y.append(y_vals[i])
#         print(prop_higher_x)        
#         print(prop_higher_y)
#         print(prop_lower_x)
#         print(prop_lower_y)
        # calc correlation 1 = the corr between low pop density to # crimes and its p value
        correlation1 = calculateCorrelations.corr(prop_lower_x, prop_lower_y)
        p1 = calculateCorrelations.p(prop_lower_x, prop_lower_y)
        # calc correlation 2 = the corr between high pop density to # crimes and its p value
        correlation2 = calculateCorrelations.corr(prop_higher_x, prop_higher_y)
        p2 = calculateCorrelations.p(prop_higher_x, prop_higher_y)
         
        finalData = dict()
        finalData['crimes_lowPropertyDensity'] = [correlation1, p1]
        finalData['crimes_highPropertyDensity'] = [correlation2, p2]
        #structure : finalData =  {'category_comparison': [correlation, p-value]}   
#         print(finalData)
              
        repo.dropCollection("calculateCorrelations")
        repo.createCollection("calculateCorrelations")
          
        repo['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations'].insert(finalData)
        repo['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations'].metadata({'complete':True})
        print(repo['bohorqux_peterg04_rocksdan_yfchen.calculateCorrelations'].metadata())
          
  
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
        repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')
#         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
#         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
#         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
#         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
#         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
# 
#         this_script = doc.agent('alg:bohorqux_peterg04_rocksdan_yfchen#calculateCorrelations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#         resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
#         get_Restaurants = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#         doc.wasAssociatedWith(get_Restaurants, this_script)
#         doc.usage(get_Restaurants, resource, startTime, None,
#                   {prov.model.PROV_TYPE:'ont:Retrieval',
#                   'ont:Query':'?type=BostonLife+Restaurants&$select=type,latitude,longitude,OPEN_DT'
#                   }
#                   )
# 
#         Restaurants = doc.entity('dat:bohorqux_peterg04_rocksdan_yfchen#Restaurants', {prov.model.PROV_LABEL:'BostonLife Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
#         doc.wasAttributedTo(Restaurants, this_script)
#         doc.wasGeneratedBy(Restaurants, calculateCorrelations, endTime)
#         doc.wasDerivedFrom(Restaurants, resource, calculateCorrelations, calculateCorrelations, calculateCorrelations)

        repo.logout()
                  
        return doc

# calculateCorrelations.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

