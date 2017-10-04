import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class ppf(dml.Algorithm):
    contributor = 'gaudiosi_raykatz'
    reads = []
    writes = ['gaudiosi_raykatz.ppf']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gaudiosi_raykatz','gaudiosi_raykatz')

        url = 'http://datamechanics.io/data/neighborhoodcrime.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        result = json.loads(response)
        r=[]

        for i in range(1,len(result)):
            d={}
            if result[i]["FIELD3"]=='Boston':
                d["Region"]=result[i]["FIELD2"]
                h=8
                d["1996-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1996-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1997-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1998-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["1999-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2000-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2001-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2002-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2003-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2004-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2005-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2006-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2007-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2008-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2009-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2010-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2011-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2012-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2013-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2014-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2015-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-08"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-09"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-10"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-11"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2016-12"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-01"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-02"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-03"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-04"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-05"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-06"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-07"]=result[i]["FIELD" + str(h)]
                h+=1
                d["2017-08"]=result[i]["FIELD" + str(h)]
                r.append(d)
        print(r)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("ppf")
        repo.createCollection("ppf")
        repo['gaudiosi_raykatz.ppf'].insert_many(r)
        repo['gaudiosi_raykatz.ppf'].metadata({'complete':True})
        print(repo['gaudiosi_raykatz.ppf'].metadata())


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
        repo.authenticate('gaudiosi_raykatz', 'gaudiosi_raykatz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

      
        this_script = doc.agent('alg:gaudiosi_raykatz#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_ppf = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_ppf, this_script)
        doc.usage(get_ppf, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        ppf = doc.entity('dat:gaudiosi_raykatz#ppf', {prov.model.PROV_LABEL:'ppf', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ppf, this_script)
        doc.wasGeneratedBy(ppf, get_ppf, endTime)
        doc.wasDerivedFrom(ppf, resource, get_ppf, get_ppf, get_ppf)

        repo.logout()
                  
        return doc

ppf.execute()
doc = ppf.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
