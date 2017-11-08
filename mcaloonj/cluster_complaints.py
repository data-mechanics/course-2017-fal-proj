import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def kmeans(M,P):
    OLD = []
    while OLD != M:
        OLD = M

        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)

        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
        return sorted(M)

class cluster_complaints(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = ['mcaloonj.cleaned_speed_complaints']
    writes = ['mcaloonj.complaint_clusters']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        repo.dropCollection('mcaloonj.complaint_clusters')
        repo.createCollection('mcaloonj.complaint_clusters')

        cleaned_complaints = repo["mcaloonj.cleaned_speed_complaints"].find()

        coords = [(float(a["longitude"]), float(a["latitude"])) for a in cleaned_complaints]

        #find min and max longitude and latitude to use as initial input to k means
        min_long = 0
        max_long = -100

        min_lat = 100
        max_lat = 0

        for x,y in coords:
            if x < min_long:
                min_long = x
            if x > max_long:
                max_long = x
            if y < min_lat:
                min_lat = y
            if y > max_lat:
                max_lat = y

        result = kmeans([(min_long, min_lat), (max_long, max_lat)], coords)

        #print (result)

        for x,y in result:
            repo["mcaloonj.complaint_clusters"].insert({"cluster":(x,y)})

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj','mcaloonj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mcj', 'mcaloonj')

        #Agent
        this_script = doc.agent('alg:mcaloonj#cluster_complaints', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})

        #Resources
        resource = doc.entity('mcj:cleaned_speed_complaints', {'prov:label': 'Cleaned Complaints', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

        #Activities
        cluster_complaints = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        #usage
        doc.wasAssociatedWith(cluster_complaints, this_script)
        doc.usage(cluster_complaints, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        #New dataset

        complaint_clusters = doc.entity('dat:mcaloonj#complaint_clusters', {prov.model.PROV_LABEL:'Complaint Clusters',prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(complaint_clusters, this_script)
        doc.wasGeneratedBy(complaint_clusters, cluster_complaints, endTime)
        doc.wasDerivedFrom(complaint_clusters, resource, cluster_complaints, cluster_complaints, cluster_complaints)

        repo.logout()
        return doc

'''
cluster_complaints.execute()
doc = cluster_complaints.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
