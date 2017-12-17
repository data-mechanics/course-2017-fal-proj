import jsonschema
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_httpauth import HTTPBasicAuth
from flask_pymongo import PyMongo
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

app.route('/')
def send_js(path):
    return send_from_directory('templates', path)


@app.route('/budgets', methods=["GET"])
def get_budgets():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('francisz_jrashaan', 'francisz_jrashaan')
    
    scores = repo['francisz_jrashaan.budgets'].find()
    
    scoreArray = []
    for score in scores:
        score.pop("_id")
        scoreArray.append(score)
    return jsonify({'results': scoreArray})



if __name__ == '__main__':
    app.run(port=3000)

