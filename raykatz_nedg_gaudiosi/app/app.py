# Authors: Ben Gaudiosi, Ray Katz, Ned Gleesin
import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo
from flask_table import Table, Col
import json
import numpy

app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)

app.secret_key = 'isok'

def get_zipinfo(zipcode):
    zipcode=str(0)+str(zipcode)
    info = list(mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode':zipcode}))[0]
# Print the htm0l
    print(str(zipcode))
    print(str(info))

    # or just {{ table }} from within a Jinja template
    return info
# def get_zipinfo(zipcode):
#     class ItemTable(Table):
#         key = Col('key')
#         value = Col('value')
# # Populate the table
#     info = mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode':zipcode})
#     items= [info(key="key1",value='value1')]
#     table = ItemTable(items)

# # Print the html
#     print(table.__html__())
# # or just {{ table }} from within a Jinja template
#     return table

def get_zips():
    info = list(mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({}).sort([("zipcode", 1)]))
    zips = []
    for zipcode in info:
        zips.append(zipcode["zipcode"])
    return zips

def get_map_data():
    return list(mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({}))

def get_gent_scores():
    return list(mongo.db.raykatz_nedg_gaudiosi.gentrification_score.find({}))

def get_standardized():
    return list(mongo.db.raykatz_nedg_gaudiosi.averages.find({}))[0]



@app.route("/", methods=['GET'])
def welcome():
    return render_template('index.html')

@app.route('/corrfinder', methods=['GET', 'POST'])
def corrfinder():
    if request.method == 'GET':
        return render_template('corrfinder.html', zipcodes=get_zips())

    elif request.method == 'POST':
        zips = request.form.getlist("chosen")
        data = []
        for zipcode in zips:
            data.append( list(mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({"zipcode":zipcode}))[0] )
        
        median_income = []
        percent_transit = []
        median_rent = []
        percent_homes_occupied = []
        percent_homes_before_1939 = []
        percent_white = []
        percent_black = []
        percent_hispanic = []
        percent_asian = []
        percent_married = []
        percent_unemployed = []
        percent_50_rent = []
        percent_poverty = []
        bus_stops = []
        subway_stops = []
        
        for i in range(len(data)):
            median_income.append(float(data[i]['median_income']))
            percent_transit.append(float(data[i]['percent_public_transit']))
            median_rent.append(float(data[i]['median_rent']))
            percent_homes_occupied.append(float(data[i]['percent_homes_occupied']))
            percent_homes_before_1939.append(float(data[i]['percent_homes_built_before_1939']))
            percent_white.append(float(data[i]['percent_white']))
            percent_black.append(float(data[i]['percent_black']))
            percent_hispanic.append(float(data[i]['percent_hispanic']))
            percent_asian.append(float(data[i]['percent_asian']))
            percent_married.append(float(data[i]['percent_married_households']))
            percent_unemployed.append(float(data[i]['percent_unemployed']))
            percent_50_rent.append(float(data[i]['percent_spending_50_rent']))
            percent_poverty.append(float(data[i]['percent_poverty']))
            bus_stops.append(float(data[i]['bus_stops']))
            subway_stops.append(float(data[i]['subway_stops']))
        
        corrs = []
        corrs.append(("Median income/median rent", numpy.corrcoef(median_income, median_rent)[0, 1]))
        
        corrs.append(("Median income/percent taking public transit", numpy.corrcoef(median_income, percent_transit)[0, 1]))
        corrs.append(("Median income/unemployed", numpy.corrcoef(median_income,percent_unemployed)[0, 1]))
        corrs.append(("Median income/percent homes occupied", numpy.corrcoef(median_income, percent_homes_occupied)[0, 1]))
        corrs.append(("Median income/percent homes built before 1939", numpy.corrcoef(median_income, percent_homes_before_1939)[0, 1]))
        corrs.append(("Median income/percent white", numpy.corrcoef(median_income, percent_white)[0, 1]))
        corrs.append(("Median income/percent black", numpy.corrcoef(median_income, percent_black)[0, 1]))
        corrs.append(("Median income/percent hispanic", numpy.corrcoef(median_income, percent_hispanic)[0, 1]))
        corrs.append(("Median income/percent asian", numpy.corrcoef(median_income, percent_asian)[0, 1]))
        corrs.append(("Median income/percent married", numpy.corrcoef(median_income,percent_married)[0, 1]))

        corrs.append(("Median rent/percent taking public transit", numpy.corrcoef(median_rent, percent_transit)[0, 1]))
        corrs.append(("Median rent/unemployed", numpy.corrcoef(median_rent,percent_unemployed)[0, 1]))
        corrs.append(("Median rent/percent spending 50% income on rent", numpy.corrcoef(median_rent,percent_50_rent)[0, 1]))
        corrs.append(("Median rent/percent homes built before 1939", numpy.corrcoef(median_rent,percent_homes_before_1939)[0, 1]))
        corrs.append(("Median rent/poverty rate", numpy.corrcoef(median_rent,percent_poverty)[0, 1]))
        corrs.append(("Median rent/bus stops", numpy.corrcoef(median_rent,bus_stops)[0, 1]))
        corrs.append(("Median rent/subway stops", numpy.corrcoef(median_rent,subway_stops)[0, 1]))
        corrs.append(("Median rent/percent married", numpy.corrcoef(median_rent,percent_married)[0, 1] ))
        
        return render_template('corrfinder.html', zipcodes=get_zips(), correlations = corrs)

@app.route("/map", methods=['GET', 'POST'])
def map():
    scores = {}
    if request.method == 'GET':
        gent_scores = get_gent_scores()
        
    
    elif request.method == 'POST':
        # Recalculate scores with custom weights
        standardized = get_standardized()
        print(request.form.get('percent_white'))
        percent_white_weight = float(request.form.get('percent_white'))
        percent_married_households_weight = float(request.form.get('percent_married_households'))
        percent_unemployed_weight = float(request.form.get('percent_unemployed'))
        percent_in_labor_force_weight = float(request.form.get('percent_in_labor_force'))
        percent_public_transit_weight = float(request.form.get('percent_public_transit'))
        median_income_weight = float(request.form.get('median_income'))
        median_rent_weight = float(request.form.get('median_rent'))
        percent_spending_50_rent_weight = float(request.form.get('percent_spending_50_rent'))
        percent_poverty_weight = float(request.form.get('percent_poverty'))
        percent_homes_built_before_1939_weight = float(request.form.get('percent_homes_built_before_1939'))
        percent_renting_weight = float(request.form.get('percent_renting'))
        subway_stops_weight = float(request.form.get('subway_stops'))
        bus_stops_weight = float(request.form.get('bus_stops'))
        gent_scores = []
        zipcodes = get_map_data()
        for zipcode in zipcodes:
            zip_score = {}
            score = 0
            score += -1*percent_white_weight*((zipcode["percent_white"] - standardized["avg_percent_white"]) / standardized["std_percent_white"])
            score += -1*percent_married_households_weight*((zipcode["percent_married_households"] - standardized["avg_percent_married_households"]) / standardized["std_percent_married_households"])
            score += percent_unemployed_weight*((zipcode["percent_unemployed"] - standardized["avg_percent_unemployed"]) / standardized["std_percent_unemployed"])
            score += percent_in_labor_force_weight*((zipcode["percent_in_labor_force"] - standardized["avg_percent_in_labor_force"]) / standardized["std_percent_in_labor_force"])
            score += percent_public_transit_weight*((zipcode["percent_public_transit"] - standardized["avg_percent_public_transit"]) / standardized["std_percent_public_transit"])
            score += -1*median_income_weight*((zipcode["median_income"] - standardized["avg_median_income"]) / standardized["std_median_income"])
            score += -1*median_rent_weight*((zipcode["median_rent"] - standardized["avg_median_rent"]) / standardized["std_median_rent"])
            score += percent_spending_50_rent_weight*((zipcode["percent_spending_50_rent"] - standardized["avg_percent_spending_50_rent"]) / standardized["std_percent_spending_50_rent"])
            score += percent_poverty_weight*((zipcode["percent_poverty"] - standardized["avg_percent_poverty"]) / standardized["std_percent_poverty"])
            score += percent_homes_built_before_1939_weight*((zipcode["percent_homes_built_before_1939"] - standardized["avg_percent_homes_built_before_1939"]) / standardized["std_percent_homes_built_before_1939"])
            score += percent_renting_weight*((zipcode["percent_renting"] - standardized["avg_percent_renting"]) / standardized["std_percent_renting"])
            score += subway_stops_weight*((zipcode["subway_stops"] - standardized["avg_subway_stops"]) / standardized["std_subway_stops"])
            score += bus_stops_weight*((zipcode["bus_stops"] - standardized["avg_bus_stops"]) / standardized["std_bus_stops"])
            zip_score["zipcode"] = zipcode["zipcode"]
            zip_score["score"] = score
            gent_scores.append(zip_score)
    
    
    # Normalize score
    max_score = -100
    min_score = 100
    for z in gent_scores:
        if z['score'] > max_score:
            max_score = z['score']
        if z['score'] < min_score:
            min_score = z['score']
    

    for z in gent_scores:
        scores[z['zipcode']] = (z['score'] - min_score) /(max_score - min_score)
    
    # Get info per zipcode
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
