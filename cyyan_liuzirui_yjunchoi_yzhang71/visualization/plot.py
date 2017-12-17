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
        
publicLoc = {}
publicXY = []
publicLoc[0] = public[0]

for i in range(1, len(public[0])):
	for o in publicLoc[0][str(i)]:
		publicXY.append(o)

x_original = [x for (x, y) in originalLoc]
y_original = [y for (x, y) in originalLoc]

x_public = [x for (x, y) in publicXY]
y_public = [y for (x, y) in publicXY]

plt.scatter(x_original, y_original, c = "b")
plt.scatter(x_public, y_public, c = "r")
plt.grid(True)
plt.show()





