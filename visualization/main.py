import flask
from flask import Flask, render_template, jsonify, request
import pymongo

app = Flask(__name__)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

@app.route('/', methods = ['GET', 'POST'])
def map():
    if request.method == 'POST':
        wardname = request.form.get('ward')
        drop = request.form.get('dropdown')

        loc = []
        dLoc = {}

        if drop == 'Original':
            pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
            for p in pLocation:
                for i in range(len(p['coordinates'])):
                    loc.append(p['coordinates'][i])
        elif drop == 'Transit':
            public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT'].find()
            dLoc[0] = public[0]
            for i in range(1, len(public[0])):
                for l in dLoc[0][str(i)]:
                    loc.append(l)
        elif drop == "MBTA":
            public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA'].find()
            dLoc[0] = public[0]
            for i in range(1, len(public[0])):
                for l in dLoc[0][str(i)]:
                    loc.append(l)
        elif drop == "BUS":
            public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByBusstop'].find()
            dLoc[0] = public[0]
            for i in range(1, len(public[0])):
                for l in dLoc[0][str(i)]:
                    loc.append(l)
        else:
            print('Something wrong with dropdown')

        return jsonify(loc)
    else:
        return render_template('index.html')

if __name__ == '__main__':
    app.run()
