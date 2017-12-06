from flask import Flask, render_template, request
from pymongo import MongoClient # Database connector
import urllib.parse
import json
#import z3_routes_interactive

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

@app.route('/visualization')
def visualization():
    # here we want to get the value of user (i.e. ?means=some-value)
    try:
        routes = int(request.args.get('routes'))
        means = int(request.args.get('means'))
    except ValueError:
        return "Please enter a valid integer input"
    if routes > 0 and means > 0:
        print("You selected {} means and {} routes".format(means, routes))
        #streets = z3_routes_interactive.find_streets(routes) z3 is stupid
        return render_template('heatmap.html')
    else:
        return "Please enter a valid number of means/routes"

if __name__ == "__main__":
    app.run()
