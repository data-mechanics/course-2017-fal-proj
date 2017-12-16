from flask import Flask, request, render_template
#from crimerate import crimerate
import json
import os


app = Flask(__name__)

@app.route('/')
def home():
    return render_template("index.html")

@app.route('/crimes')
def rendercrimes():
    #data = crimerate.execute()
    return render_template('crimeheatmap.html')

@app.route('/kmeans')
def kmeans():
    return render_template('kmeans.html')
