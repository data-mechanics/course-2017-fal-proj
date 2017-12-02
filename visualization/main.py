import flask
from flask import Flask, render_template
import pymongo


app = Flask(__name__)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

@app.route('/',methods=['GET'])
def hello():
    return render_template('index.html')

@app.route('/',methods=['POST'])
def register_user():
	try:
		wardname = request.form.get('ward')
		drop = request.form.get('dropdown')
	except:
		print("couldn't find all tokens")
		return render_template('index.html')

	if drop == "Original":
		pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
		Loc = []
		for o in pLocation:
		    for i in range(0,len(o['coordinates'])):
		        Loc.append(o['coordinates'][i])
	elif drop == "Transit":
		public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT'].find()
		publicLoc = {}
		Loc = []
		publicLoc[0] = public[0]
		for i in range(1, len(public[0])):
			for o in publicLoc[0][str(i)]:
				Loc.append(o)
	elif drop == "MBTA":
		optByMBTA = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA'].find()
		MBTALoc = {}
		Loc = []
		MBTALoc[0] = optByMBTA[0]
		for i in range(1, len(optByMBTA[0])):
			for o in MBTALoc[0][str(i)]:
				Loc.append(o)
	elif drop == "BUS":
		optByBusstop = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByBusstop'].find()
		BUSLoc = {}
		Loc = []
		BUSLoc[0] = optByBusstop[0]
		for i in range(1, len(optByBusstop[0])):
			for o in BUSLoc[0][str(i)]:
				Loc.append(o)
	else:
		print("Error")
		return render_template('index.html')
	





@app.route('/score')
def score():
    return render_template('score.html')

if __name__ == '__main__':
    app.run()















