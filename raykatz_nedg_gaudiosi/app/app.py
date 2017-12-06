	# Authors: Ben Gaudiosi, Ray Katz, Ned Gleesin
import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo
from flask_table import Table, Col
import json

app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)

app.secret_key = 'isok'

def get_zipinfo(zipcode):
    info = mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode':zipcode})
    class ItemTable(Table):
        name = Col('key')
        description = Col('value')
    # Populate the table
    table = ItemTable(info)

    # Print the html
    print(table.__html__())
    # or just {{ table }} from within a Jinja template
    return table


def get_map_data():
    return list(mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({}))

def get_gent_scores():
    return list(mongo.db.raykatz_nedg_gaudiosi.gentrification_score.find({}))


# index page
@app.route("/", methods=['GET'])
def welcome():
    return render_template('index.html')

@app.route('/corrfinder', methods=['GET'])
def corrfinder():
    return render_template('corrfinder.html')    

@app.route("/map", methods=['GET'])
def map():
    gent_scores = get_gent_scores()
    scores = {}
    max_score = -100
    min_score = 100
    for z in gent_scores:
        if z['score'] > max_score:
            max_score = z['score']
        if z['score'] < min_score:
            min_score = z['score']

    for z in gent_scores:
        scores[z['zipcode']] = (z['score'] - min_score) /(max_score - min_score)
    
    zipinfo = {}
    zipcodes = get_map_data()
    for zipcode in zipcodes:
        current ={}
        current["percent_white"] = zipcode["percent_white"]
        current["percent_black"] = zipcode["percent_black"]
        current["percent_asian"] = zipcode["percent_asian"]
        current["percent_hispanic"] = zipcode["percent_hispanic"]
        current["percent_married_households"] = zipcode["percent_married_households"]
        current["percent_unemployed"] = zipcode["percent_unemployed"]
        current["percent_poverty"] = zipcode["percent_poverty"]
        current["percent_homes_occupied"] = zipcode["percent_homes_occupied"]
        current["percent_homes_vacant"] = zipcode["percent_homes_vacant"]
        current["percent_homes_built_before_1939"] = zipcode["percent_homes_built_before_1939"]
        current["percent_renting"] = zipcode["percent_renting"]
        current["median_income"] = zipcode["median_income"]
        current["median_rent"] = zipcode["median_rent"]
        current["subway_stops"] = zipcode["subway_stops"]
        current["commuter_stops"] = zipcode["commuter_stops"]
        current["bus_stops"] = zipcode["bus_stops"]
        zipinfo[zipcode["zipcode"]] = current

    return render_template('map.html', scores=scores, zipinfo=zipinfo)
 


@app.route("/stattrack <int:zip>", methods=['GET'])
def stattrack(zip):
    zipinfo=get_zipinfo(zip)
    return render_template('stattrack.html',info=zipinfo)

if __name__ == "__main__":
    app.run(port=5000, debug=True)
