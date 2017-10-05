from urllib import request, parse
import json
import dml, prov.model
import datetime, uuid

""" 
Skelton file provided by lapets@bu.edu
Heavily modified by bmroach@bu.edu

Yearly rainfall data by Boston neighborhood

Development notes:
-Currently not returning correct data, but have data from *somewhere
"""

class retrieve_rainfall(dml.Algorithm):
    contributor = 'bmroach'
    reads = []
    writes = ['bmroach.rainfall']

    @staticmethod
    def execute(trial = False, log=False, htmlDump = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bmroach', 'bmroach')
        repo.dropCollection("rainfall")
        repo.createCollection("rainfall")
        
        # Do retrieving of data
        rainfallList = []
        years = [year for year in range(1999,2018)]
        
        url = "http://www.bwsc.org/COMMUNITY/rainfall/telog_rainfall/rf_yearly.asp"
        
        for year in years:
            p = [(('reqyear', str(year)))]
            data = parse.urlencode(p).encode()
            req = request.Request(url, data=data)
            res = request.urlopen(req).read().decode('iso-8859-1')

            if htmlDump:
                with open("./webOutput/"+str(year)+".html", 'w') as f:
                    f.write(res)
            
            #Extracting the table with the desired information
            tableStart = res.index("""<table id='tblRainfallData'>""")
            res = res[tableStart:]
            tableEnd = res.index("</div>")
            resSection = res[:tableEnd].split('\n')

            regions = {}
            for line in resSection:
                exec1 = False
                if "Allston" in line:
                    r = "Allston"
                    exec1 = True
                elif "Charleston" in line:
                    r = "Charleston"
                    exec1 = True
                elif "Dorch-Adams" in line:
                    r = "Dorch-Adams"
                    exec1 = True
                elif "Dorch-Talbot" in line:
                    r = "Dorch-Talbot"
                    exec1 = True
                elif "Hyde Park" in line:
                    r = "Hyde Park"
                    exec1 = True
                elif "Longwood" in line:
                    r = "Longwood"
                    exec1 = True
                elif "Roslindale" in line:
                    r = "Roslindale"
                    exec1 = True
                
                elif "Union Park PS" in line: #this line also has the average
                    r = "Union Park PS"
                    exec1=True
                    lineCopy = line
                    #extract the average across all neighborhoods from this line
                    start = lineCopy.index("Average") + 33
                    lineCopy = lineCopy[start:]
                    end = lineCopy.index("</b></th>")
                    val = lineCopy[:end]
                    regions['Average'] = val
                    

                if exec1: #gets the value for a neighborgood and adds it to the dict
                    start = line.index("'right'>")+8
                    line = line[start:]
                    end = line.index("</td>")
                    line=line[:end]
                    regions[r] = line

            #add year to db list
            rainfallList.append( {str(year) : regions} ) 
            
            if log:
                print("\nYear:", year)
                for key, val in regions.items():
                    print(key, '\t', val)

        repo["bmroach.rainfall"].insert_many( rainfallList )
        repo['bmroach.rainfall'].metadata({'complete':True})
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

        
                  
        return 





retrieve_rainfall.execute(log=True)

# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
