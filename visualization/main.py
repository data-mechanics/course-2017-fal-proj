import flask
from flask import Flask, render_template
import pymongo


app = Flask(__name__)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

@app.route('/')
def hello():
    return render_template('hello.html')

@app.route('/score')
def score():
    return render_template('score.html')
