import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict


def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]

def finde(x,y):
    all_e = []
    for k,v in x.items():
        # print(k)
        # print(v)
        for c in y:
            m = distance(v,c)
            if m < 2:
                all_e.append((k,c))
    return all_e


def cro(x,y):
    all_cord = []
    for key,value in x.items():
        for c2 in y:
            all_cord.append((key,distance(value,c2)))
    return all_cord

def project(R, p):
    return [p(t) for t in R]

def comb(ls):
    result ={}
    for k,v in ls:
        if k in result:
            if v not in result[k]:
                result[k].append(v)
        else:
            result[k] = []

    # print(result)
    return result





def distance(origin, destination):
    lat1 = origin[0] 
    lon1 = origin[1]
    lat2 = destination[0] 
    lon2 = destination[1]
    radius = 3959 #miles

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d


def counter(ls):
    c = 0
    result =[]
    for k,v in ls.items():
        for i in v:
            if i <=1.5:
                c += 1
            else:
                c == c
        result.append(((k),c))
        c = 0
    return dict(result)








class schoolfinal(dml.Algorithm):
    contributor = 'eileenli_xtq_yidingou'
    reads = ['eileenli_xtq_yidingou.schools', 'eileenli_xtq_yidingou.comfort', 'eileenli_xtq_yidingou.safety', 'eileenli_xtq_yidingou.traffic']
    writes = ['eileenli_xtq_yidingou.schoolfinal_data']


    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')

        # loads the collection
        SC = repo['eileenli_xtq_yidingou.schools'].find()
        CM = repo['eileenli_xtq_yidingou.comfort'].find()
        SF = repo['eileenli_xtq_yidingou.safety'].find()
        TR = repo['eileenli_xtq_yidingou.traffic'].find()

        final_dic = {}


        name_cord = {}
        c = []
        l = []
        for k in SC:
            #print(k)
            for v in k['properties']:
                # print(v)
                # for i in v:
                    #print(i)
                if v == "Name":
                    # print(v)
                    c.append(k['properties']["Name"])
                    # name = i['NAME']
            for m in k['geometry']:
                if m =="coordinates":
                    # print(m)
                    l.append(k['geometry']["coordinates"])


        for i in range(len(c)):
            name_cord.update({c[i]:l[i]})            

        # for m in SC:
        #     print(m)
        #     for n in m:
        #         print(n)
        #         if n == "coordinates":
        #             print(n)
                        
        entertainment_list = []

        for k in CM:
            # print(k)
            for v in k:
                if v == "entertainment":
                    # print(v)
                    entertainment_list = k["entertainment"]

        en_sc = finde(name_cord, entertainment_list)
        en_sc = comb(en_sc) #school: coordinate of entertainment within 2 miles
        print('entertainment finished')

        CM.rewind()


        restaurants_list = []

        print('restaurant start')

        # print(CM)
        for k in CM:
            # print(k)
            for v in k:
                if v == "restaurants":
                    # print(v)
                    restaurants_list = k["restaurants"]

        re_sc = finde(name_cord, restaurants_list)
        re_sc = comb(re_sc)#school: coordinates of restaurants within 2 miles


        #crime starts
        crime_list = []

        for k in SF:
            for v in k:
                if v == "crimes":
                    # print(v)
                    crime_list = k["crimes"]

        cr_sc = finde(name_cord, crime_list)
        cr_sc = comb(cr_sc)#school: coordinates of crimes within 2 miles

        #hospital starts
        SF.rewind()

        hospital_list = []

        for k in SF:
            for v in k:
                if v == "hospitals":
                    # print(v)
                    hospital_list = k["hospitals"]

        hos_sc = finde(name_cord, hospital_list)
        hos_sc = comb(hos_sc)#school: coordinates of hospitals within 2 miles

        #crash starts
        SF.rewind()

        crash_list = []

        for k in SF:
            for v in k:
                if v == "crash":
                    # print(v)
                    crash_list = k["crash"]

        crash_sc = finde(name_cord, crash_list)
        crash_sc = comb(crash_sc)#school: coordinates of crashs within 2 miles

        #hubway starts
        hubway_list = []

        for k in TR:
            for v in k:
                if v == "hubway":
                    # print(v)
                    hubway_list = k["hubway"]

        hub_sc = finde(name_cord, hubway_list)
        hub_sc = comb(hub_sc)#school: coordinates of hubways within 2 miles

        TR.rewind()

        #signals starts
        signal_list = []

        for k in TR:
            for v in k:
                if v == "signals":
                    # print(v)
                    signal_list = k["signals"]

        sig_sc = finde(name_cord, signal_list)
        sig_sc = comb(sig_sc)#school: coordinates of signals within 2 miles

        TR.rewind()

        #MBTA starts
        MBTA_list = []

        for k in TR:
            for v in k:
                if v == "MBTA":
                    # print(v)
                    MBTA_list = k["MBTA"]

        mb_sc = finde(name_cord, MBTA_list)
        mb_sc = comb(mb_sc)#school: coordinates of MBTA within 2 miles
                

                
                










        print(mb_sc)






        # en_cord = {}
        # l = []
        # for k in EN:
        #     for i in k:
        #         if i == 'businessname' or i == 'location':
        #             try:
        #                 nt = k['location'].strip("()").split(",")
        #                 l = [float(nt[1]),float(nt[0])]
        #                 en_cord.setdefault(k['businessname'],l).append()
        #             except:
        #                 pass

        # result = []
        # for k,v in en_cord.items():
        #     if v != 'NULL':
        #         try:
        #             result.append((k,v))
        #         except KeyError:
        #             result == result
        # en_cord = dict(result)



        # crime_cord = []
        # for key in CR:
        #     for i in key['location']:
        #         if i == 'coordinates':
        #             try:
        #                 crime_cord.append((key['location']['coordinates']))
                        
        #             except:
        #                 pass
        # all_cord = cro(en_cord,crime_cord)
        # all_cord = comb(all_cord)
        # re_cord = counter(all_cord)


        repo.dropCollection("schoolfinal")
        repo.createCollection("schoolfinal")

        repo['eileenli_xtq_yidingou.schoolfinal'].insert_one(re_sc)
        setdis=[]

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('eileenli_xtq_yidingou', 'eileenli_xtq_yidingou')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<fileEnCrime> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:#schoolfinal',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_schoolss = doc.entity('dat:eileenli_xtq_yidingou#schoolss',
                                             {'prov:label': 'schoolss',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_crime = doc.entity('dat:eileenli_xtq_yidingou#crime',
                                             {'prov:label': 'crime',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})
        

        get_EnCrime = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_EnCrime, this_script)
        doc.usage(get_EnCrime, resource_schoolss, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_EnCrime, resource_crime, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        

        EnCrime = doc.entity('dat:eileenli_xtq_yidingou#schoolfinal',
                          {prov.model.PROV_LABEL: 'schools Cirme',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(EnCrime, this_script)
        doc.wasGeneratedBy(EnCrime, get_EnCrime, endTime)
        doc.wasDerivedFrom(EnCrime, resource_schoolss, get_EnCrime, get_EnCrime, get_EnCrime)
        doc.wasDerivedFrom(EnCrime, resource_crime, get_EnCrime, get_EnCrime, get_EnCrime)
        
        repo.logout()

        return doc

schoolfinal.execute()
doc = schoolfinal.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof