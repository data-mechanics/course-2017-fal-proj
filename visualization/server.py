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
print(db.collection_names())

cursor = db['bkin18_cjoe_klovett_sbrz.formatted_coords'].find()


'''
for formatted_coord in cursor:
    print(formatted_coord)
'''

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

        '''
        Formats kmeans data to be used in visualization
        '''
        kMeansCoordinateList = []

        for kmeans_info in db['bkin18_cjoe_klovett_sbrz.closest_buildings_to_centroids'].find():
            coordinate_list = []
            centroid_coordinates = kmeans_info['NEARBY_CENTROID']
            nearest_building_info = kmeans_info['_id']
            nearest_building_coordinates = [{'lat': nearest_building_info['LATITUDE'], 'lng': nearest_building_info['LONGITUDE']}]
            coordinate_list.append(centroid_coordinates)
            coordinate_list.append(nearest_building_coordinates)
            kMeansCoordinateList.append(coordinate_list)

        kMeansData = {'coordinates': str(kMeansCoordinateList)}




        print("You selected {} means and {} routes".format(means, routes))
        #streets = z3_routes_interactive.find_streets(routes) z3 is stupid
        #we = list(db['bkin18_cjoe_klovett_sbrz.formatted_coords'].find())
        #print(we)
        passTest = {'test': str(kMeansCoordinateList)}
        #passTest = kMeansDictionary
        return render_template('heatmap.html', kMeansData=kMeansData)
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
        return "Please enter a valid number of means/routes"

if __name__ == "__main__":
    app.run()
