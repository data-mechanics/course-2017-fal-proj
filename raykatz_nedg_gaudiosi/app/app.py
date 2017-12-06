	# Authors: Ben Gaudiosi, Ray Katz, Ned Gleesin
import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo
import flask.ext.login as flask_login

import json

app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)

app.secret_key = 'isok'

def get_zipinfo(zipcode):
    info = mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode':zipcode})
    Table = []
    for key, value in info:
        temp = []
        temp.extend([key,value])  #Note that this will change depending on the structure of your dictionary
        Table.append(temp)
    return Table

# index page
@app.route("/", methods=['GET'])
def welcome():
    return render_template('index.html')

@app.route('/corrfinder', methods=['GET'])
def corrfinder():
    return render_template('corrfinder.html')    

@app.route("/map", methods=['GET'])
def map():
    return render_template('map.html')


@app.route("/stattrack <int:zip>", methods=['GET'])
def stattrack(zip):
    zipinfo=get_zipinfo(zip)
    return render_template('stattrack.html',info=zipinfo)

if __name__ == "__main__":
    # this is invoked when in the shell  you run
    # $ python app.py
    app.run(port=5000, debug=True)
