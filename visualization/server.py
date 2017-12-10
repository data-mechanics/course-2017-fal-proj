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
        markers = str(request.args.get('markers'))
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

    if (markers == "draw"):
        markerURLs = []
        baseURL = 'https://maps.googleapis.com/maps/api/geocode/json?address='
        for street in streets:
            url = baseURL + street.replace(" ", "+") + '+Boston+Massachusetts&key=' + api_key
            markerURLs.append(url)
        print(markerURLs)
        
        markerCoordList = []
        for i in range(len(markerURLs)):
            url = markerURLs[i]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            markerCoord = r['results'][0]['geometry']['location']
            print(i, "out of", len(markerURLs), "markers generated.")
            markerCoordList.append(markerCoord)

        dataMapper = dataMapper.replace('{{markerData}}', str(markerCoordList))
    else:
        dataMapper = dataMapper.replace('{{markerData}}', str([]))
        

    return dataMapper


if __name__ == "__main__":
    app.run()
