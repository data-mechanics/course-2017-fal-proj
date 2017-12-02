import matplotlib.pyplot as plt
import pymongo

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

original = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
busstop = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByBusstop'].find()
mbta = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA'].find()
public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT'].find()

originalLoc = []
for o in original:
    for i in range(0,len(o['coordinates'])):
        originalLoc.append(o['coordinates'][i])

print(originalLoc)
