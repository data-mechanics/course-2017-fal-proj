import flask
import json
from flask import Flask, render_template, jsonify, request
import pymongo


# To run the program: FLASK_APP=main.py flask run

app = Flask(__name__)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

@app.route('/')
def hello():
    return render_template('home.html')

@app.route('/marker')
def getMarker():
    marker = json.load(open('latlng.json', 'r'))
    return jsonify(marker)

@app.route('/map', methods = ['POST'])
def map():
    wardname = request.form.get('ward')
    drop = request.form.get('dropdown')

    l = []
    loc = []
    dLoc = {}

    if drop == 'Original':
        pLocation = repo['cyyan_liuzirui_yjunchoi_yzhang71.pollingLocation'].find()
        for p in pLocation:
            for i in range(len(p['coordinates'])):
                l = [p['coordinates'][i][1],  p['coordinates'][i][0]]
                loc.append(l)
    elif drop == 'Transit':
        public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByPublicT'].find()
        dLoc[0] = public[0]
        for i in range(1, len(public[0])):
            for o in dLoc[0][str(i)]:
                l = [o[1], o[0]]
                loc.append(l)
    elif drop == "MBTA":
        public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByMBTA'].find()
        dLoc[0] = public[0]
        for i in range(1, len(public[0])):
            for o in dLoc[0][str(i)]:
                l = [o[1], o[0]]
                loc.append(l)
    elif drop == "BUS":
        public = repo['cyyan_liuzirui_yjunchoi_yzhang71.optByBusstop'].find()
        dLoc[0] = public[0]
        for i in range(1, len(public[0])):
            for o in dLoc[0][str(i)]:
                l = [o[1], o[0]]
                loc.append(l)
    else:
        print('Something wrong with dropdown')

    result = {}

    for i in range(255):
        result[str(i)] = loc[i]

    with open('latlng.json', 'w') as makeFile:
        json.dump(result, makeFile)

    marker = getMarker()
    return render_template('map.html')


@app.route('/score', methods = ['GET', 'POST'])
def score_board():
    if request.method == 'POST':
        drop1 = request.form.get('dropdown1')
        drop2 = request.form.get('dropdown2')
        print("this is drop1", drop1)
        print("this is drop2", drop2)

        NewDrop1 = score_drop(drop1)
        NewDrop2 = score_drop(drop2)

        if NewDrop1 == "error" or NewDrop2 == "error":
            print('Something wrong with dropdown')
        else:
            drop1 = NewDrop1
            drop2 = NewDrop2

        scoring = repo['cyyan_liuzirui_yjunchoi_yzhang71.scoringLocation'].find()
        s = [[], []]
        s[0] = dict_to_list(scoring[0][drop1], drop1)
        s[1] = dict_to_list(scoring[0][drop2], drop2)
        # print(type(scoring[0][drop1]))
        print(s[0])
        return render_template('score.html', message = s)

    else:
        return render_template('score.html')

def score_drop(drop):
    if drop == "Original":
        return "pollingLocation"
    elif drop == "Transit":
        return "optByPublicT"
    elif drop == "MBTA":
        return "optByMBTA"
    elif drop == "BUS":
        return "optByBusstop"
    else:
        return "error"


def dict_to_list(dic, id):
    l = []
    l.append(id)
    l.append(dic['avg'])
    l.append(dic['stddev'])
    l.append(dic['lowCI95'])
    l.append(dic['highCI95'])
    return l



if __name__ == '__main__':
    app.run()
