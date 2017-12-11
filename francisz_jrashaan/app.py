import jsonschema
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask.ext.httpauth import HTTPBasicAuth
from flask.ext.pymongo import PyMongo
import dml

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'repo'
app.config['MONGO_USERNAME'] = 'francisz_jrashaan'
app.config['MONGO_PASSWORD'] = 'francisz_jrashaan'

mongo = PyMongo(app, config_prefix='MONGO')

auth = HTTPBasicAuth()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/score', methods=["GET"])
def get_big_belly():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('andradej_chojoe', 'andradej_chojoe')
    
    bigbellyinfo = repo['andradej_chojoe.bigbelly_transf'].find()
    
    bigbelly_entries = []
    for data in bigbellyinfo:
        bigbelly_entries.append({ 'location': data['location'], 'count': data['count'], 'percentage': data['percentage']})
    
    return jsonify({'results': bigbelly_entries})

