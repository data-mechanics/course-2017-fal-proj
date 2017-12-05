from flask import Flask, render_template, request
from pymongo import MongoClient # Database connector
import urllib.parse
import json

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
    routes = int(request.args.get('routes'))
    if routes > 0:
        return "You selected {} routes".format(routes)
    else:
        return "Please enter a valid number of routes"


if __name__ == "__main__":
    app.run()
