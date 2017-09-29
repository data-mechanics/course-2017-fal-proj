from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class getHealthInspectionReports(dml.Algorithm):
    contributor = 'biel_otis'
    reads = []
    writes = ['biel_otis.getHealthInspection']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('biel_otis', 'biel_otis')

        dataSets = {'healthInspectionsBoston': 'https://data.boston.gov/datastore/odata3.0/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c'}
        for ds in dataSets:
            url = dataSets[ds]
            response = urlopen(url).read().decode("utf-8")
            o = xmltodict.parse(response)
            r = json.loads(o)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropPermanent(ds)
            repo.createPermanent(ds)
            repo['biel_otis.' + ds].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        """
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # writes = ['houset_karamy.policeStations','houset_karamy.crimeReportsBoston', 'houset_karamy.crimeReportsCambridge', 'houset_karamy.policeCarRoutesCambridge', 'houset_karamy.policeWalkingRoutesCambridge','houset_karamy.realTimeTravelMassdot']


        this_script = doc.agent('alg:houset_karamy#getCrimeReportsBoston',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource1 = doc.entity('bdp:crime',
                               {'prov:label': 'Crime Reports Boston', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        get_crimeReportsBoston = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #         get_hospitals = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=ad&?$select=ad,name'})

        #         get_realTimeTravelMassDot = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crimeReportsBoston, this_script)

        #         doc.wasAssociatedWith(get_realTimeTravelMassDot, this_script)

        doc.usage(get_crimeReportsBoston, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        crimeReportsBoston = doc.entity('dat:houset_karamy#crimeReportsBoston',
                                        {prov.model.PROV_LABEL: 'Crime Reports Boston',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crimeReportsBoston, this_script)
        doc.wasGeneratedBy(crimeReportsBoston, get_crimeReportsBoston, endTime)
        doc.wasDerivedFrom(crimeReportsBoston, resource1, get_crimeReportsBoston, get_crimeReportsBoston,
                           get_crimeReportsBoston)

        repo.logout()
"""
        return doc


        get.execute()
        doc = get.provenance()
        (doc.get_provn())
        print(json.dumps(json.loads(doc.serialize()), indent=4))


        ## eof
        