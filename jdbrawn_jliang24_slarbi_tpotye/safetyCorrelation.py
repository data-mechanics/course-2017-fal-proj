
import dml
import prov.model
import datetime
import uuid
import gpxpy.geo
from random import shuffle
from math import sqrt

class safetyCorrelation(dml.Algorithm):
    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def avg(x): # Average
        return sum(x)/len(x)

    def stddev(x): # Standard deviation.
        m = safetyCorrelation.avg(x)
        return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

    def cov(x, y): # Covariance.
        return sum([(xi-safetyCorrelation.avg(x))*(yi-safetyCorrelation.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

    def corr(x, y): # Correlation coefficient.
        if safetyCorrelation.stddev(x)*safetyCorrelation.stddev(y) != 0:
            return safetyCorrelation.cov(x, y)/(safetyCorrelation.stddev(x)*safetyCorrelation.stddev(y))

    def p(x, y):
        c0 = safetyCorrelation.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = safetyCorrelation.permute(y)
            corrs.append(safetyCorrelation.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)

    contributor = 'jdbrawn_jliang24_slarbi_tpotye'
    reads = ['jdbrawn_jliang24_slarbi_tpotye.policeAnalysis']
    writes = ['jdbrawn_jliang24_slarbi_tpotye.safetyCorrelation']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        policeAnalysis = repo['jdbrawn_jliang24_slarbi_tpotye.policeAnalysis'] 


        data = []
        for entry in policeAnalysis.find():
            data.append((entry['Safety Score'], entry['Num Police']))
        
        x = [xi for (xi, yi) in data]
        y = [yi for (xi, yi) in data]
        
        c= safetyCorrelation.corr(x,y)
        pval= safetyCorrelation.p(x,y)
        print("Correlation between safety score and proximity to police stations:")
        print("Correlation: "+ str(c), "Pval: " + str(pval))

        #format it for MongoDB
        correlation_data = []

        correlation_data.append({'Correlation': c, 'Pval': pval})

        repo.dropCollection('safetyCorrelation')
        repo.createCollection('safetyCorrelation')
        repo['jdbrawn_jliang24_slarbi_tpotye.safetyCorrelation'].insert_many(correlation_data)

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
        repo.authenticate('jdbrawn_jliang24_slarbi_tpotye', 'jdbrawn_jliang24_slarbi_tpotye')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/api/action/datastore_search?resource_id=')
        doc.add_namespace('591', 'http://datamechanics.io/data/jdbrawn_jliang24_slarbi_tpotye/')
        doc.add_namespace('bdp1', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jdbrawn_jliang24_slarbi_tpotye#safetyCorrelation', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_policeAnalysis = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#policeAnalysis', {'prov:label': 'Police Station and Schools', prov.model.PROV_TYPE: 'ont:DataSet'})


        get_safetyCorrelation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_safetyCorrelation, this_script)

        doc.usage(get_safetyCorrelation, resource_policeAnalysis, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        correlation = doc.entity('dat:jdbrawn_jliang24_slarbi_tpotye#safetyCorrelation', {prov.model.PROV_LABEL: 'Safety Correlation', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        doc.wasAttributedTo(correlation, this_script)
        doc.wasGeneratedBy(correlation, get_safetyCorrelation, endTime)
        doc.wasDerivedFrom(correlation, resource_policeAnalysis, get_safetyCorrelation, get_safetyCorrelation, get_safetyCorrelation)

        repo.logout()

        return doc
