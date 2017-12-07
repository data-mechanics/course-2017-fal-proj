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

roads = [x for x in db['bkin18_cjoe_klovett_sbrz.emergency_routes_dict'].find()][0]

@app.route("/")
def index():
    return render_template('index.html')

@app.route('/visualization')
def visualization():
    # here we want to get the value of user (i.e. ?means=some-value)
    try:
        routes = int(request.args.get('routes'))
        means = int(request.args.get('means'))
    except ValueError:
        return "Please enter a valid integer input"

    if routes > 0 and means > 0:
        coordinates = []
        used_streets = z3_routes_interactive.find_streets(routes).split(', ')

        for street in used_streets:
            coordinates.append(roads[street])

        return "{}".format(coordinates)
        #return render_template('heatmap.html')
    else:
        return "Please enter a valid number of means"


def routes():
    # here we want to get the value of user (i.e. ?means=some-value)
    routes = int(request.args.get('routes'))

if __name__ == "__main__":
    app.run()
