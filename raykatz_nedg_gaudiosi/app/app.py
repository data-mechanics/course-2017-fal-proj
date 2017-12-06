	# Authors: Ben Gaudiosi, Ray Katz, Ned Gleesin
import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo
import flask.ext.login as flask_login
from flask_table import Table, Col
import json

app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)

app.secret_key = 'isok'

def get_zipinfo(zipcode):
    info = list(mongo.db.raykatz_nedg_gaudiosi.zipcode_info.find({'zipcode':zipcode}))
    class ItemTable(Table):
        name = Col('key')
        description = Col('value')
# Populate the table
    table = ItemTable(info)

# Print the htm0l
    print(str(zipcode))
    print(str(info))
    print(table.__html__())
# or just {{ table }} from within a Jinja template
    return table
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
