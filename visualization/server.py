from flask import Flask, render_template, request
from pymongo import MongoClient  # Database connector
import urllib.parse
import json
import z3_routes_interactive
import centroids_interactive
import pdb
import dml
import ast

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
    authData = dml.auth;
    # here we want to get the value of user (i.e. ?means=some-value)
    try:
        num_routes = int(request.args.get('num_routes'))
        means = int(request.args.get('means'))
    except ValueError:
        return "Please enter a valid integer input"

    if (num_routes <= 112 or num_routes >= 144):
        num_routes = 112

    if means <= 0:
        means = 1

    kMeansCoordList, routeCoordList, placeId = [], [], []

    # retrieve centroids for specified number of means
    kMeansCoordList = str(centroids_interactive.find_centroids(means))

    # retrieve streets for requested input
    streets = z3_routes_interactive.find_streets(num_routes).split(', ')

    # retrieve google maps api key
    key = authData['services']['googleportal']['key']
    print("Key retrieved from auth.json:", key)

    # append coordinates for given streets
    # append requests for google maps place id
    for street in streets:
        routeCoordList.append(routes[street])

    # Retrieve geometric points and google maps placeIds for each emergency route 
    lines = open("route_locations.txt", "r")
    placeIdCoords = [x for x in lines.readlines()]

    for coordinate in placeIdCoords:
        placeId.append(ast.literal_eval(coordinate[:-1]))

    # format to make javascript readable
    routeCoordList = str(routeCoordList)
    routeCoordList = routeCoordList.replace("'lat'", "lat")
    routeCoordList = routeCoordList.replace("'lng'", "lng")

    # render html with necessary data inputs
    dataMapper = open('templates/heatmap.html', 'r').read()
    dataMapper = dataMapper.replace('{{routesData}}', routeCoordList)
    dataMapper = dataMapper.replace('{{kMeansData}}', kMeansCoordList)
    dataMapper = dataMapper.replace('{{placeId}}', str(placeId))
    dataMapper = dataMapper.replace('{{key}}', key)

    return dataMapper


if __name__ == "__main__":
    app.run()
