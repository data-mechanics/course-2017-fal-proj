import flask
from flask import Flask, render_template, jsonify, request
import pymongo

# To run the program: FLASK_APP=main.py flask run

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
        s = {}
        s[drop1] = scoring[0][drop1]
        s[drop2] = scoring[0][drop2]
        print(s)
        return render_template('score.html') 

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



if __name__ == '__main__':
    app.run()
