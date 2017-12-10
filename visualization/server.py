from flask import Flask, render_template, request
from pymongo import MongoClient  # Database connector
import urllib.parse
import json
import z3_routes_interactive
import centroids_interactive
import pdb
import dml

app = Flask(__name__)

username = urllib.parse.quote_plus('bkin18_cjoe_klovett_sbrz')
password = urllib.parse.quote_plus('bkin18_cjoe_klovett_sbrz')
client = MongoClient('localhost', 27017, username=username, password=password,
                     authSource="repo")  # Configure the connection to the database
db = client['repo']  # Select the database

routes = [x for x in db['bkin18_cjoe_klovett_sbrz.emergency_routes_dict'].find()][0]


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/snow")
def index_snow():
    return render_template('index-snow.html')


@app.route('/visualization')
def visualization():
    # here we want to get the value of user (i.e. ?means=some-value)
    try:
        api_key = str(request.args.get('api_key'))
        num_routes = int(request.args.get('num_routes'))
        means = int(request.args.get('means'))
    except ValueError:
        return "Please enter a valid input"

    if (num_routes <= 112 or num_routes >= 144):
        return "Please enter a valid route input"

    if (means <= 0 or means > 21):
        return "Please enter a valid means input"

    '''
    Formats kmeans data to be used in visualization
    '''
    kMeansCoordList, routeCoordList = [], []

    kMeansCoordList = str(centroids_interactive.find_centroids(means))

    streets = z3_routes_interactive.find_streets(num_routes).split(', ')

    for street in streets:
        routeCoordList.append(routes[street])

    routeCoordList = str(routeCoordList)
    routeCoordList = routeCoordList.replace("'lat'", "lat")
    routeCoordList = routeCoordList.replace("'lng'", "lng")

    dataMapper = open('templates/heatmap.html', 'r').read()

    dataMapper = dataMapper.replace('{{apiKeyData}}', api_key)
    dataMapper = dataMapper.replace('{{routesData}}', routeCoordList)
    dataMapper = dataMapper.replace('{{kMeansData}}', kMeansCoordList)

    return dataMapper


if __name__ == "__main__":
    app.run()
