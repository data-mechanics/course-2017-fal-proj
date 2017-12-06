from flask import Flask, render_template, request
from pymongo import MongoClient # Database connector
import urllib.parse
import json
import z3_routes_interactive
import pdb
import dml

app = Flask(__name__)
username = urllib.parse.quote_plus('bkin18_cjoe_klovett_sbrz')
password = urllib.parse.quote_plus('bkin18_cjoe_klovett_sbrz')
client = MongoClient('localhost', 27017, username=username, password=password, authSource="repo")    #Configure the connection to the database
db = client['repo'] #Select the database
# print(db.collection_names())

cursor = db.bkin18_cjoe_klovett_sbrz.traffic_signals.find()

for traffic_signal in cursor:
    print(traffic_signal['properties']['Location'])
    
@app.route("/")
def index():
    return render_template('index.html')


@app.route('/means')
def means():
    # here we want to get the value of user (i.e. ?means=some-value)
    means = int(request.args.get('means'))
    if means > 0:
        # return "You selected {} means".format(means)
        return render_template('heatmap.html')
    else:
        return "Please enter a valid number of means"


@app.route('/emergency_routes')
def routes():
    # here we want to get the value of user (i.e. ?means=some-value)
  
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('bkin18_cjoe_klovett_sbrz', 'bkin18_cjoe_klovett_sbrz')
    db = client.repo

    roads = [x for x in db['bkin18_cjoe_klovett_sbrz.emergency_routes_dict'].find()]
    roads = roads[0]

    routes = int(request.args.get('routes'))
    coordinates = []
    if routes > 0:
        streets = z3_routes_interactive.find_streets(routes).split(', ')
        for street in streets:
            coordinates.append(roads[street])
        return "{}".format(coordinates)
    else:
        return "Please enter a valid number of routes"


if __name__ == "__main__":
    app.run()
