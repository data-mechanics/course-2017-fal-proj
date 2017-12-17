import flask
import json
from flask import Flask, render_template, jsonify, request
import pymongo




app = Flask(__name__)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('lmy1031_zhuoshu', 'lmy1031_zhuoshu')

@app.route('/')
def homepage():
    return render_template('proj3.html')



@app.route('/map', methods = ['GET'])
def map():
    wardname = request.form.get('ward')
    drop = request.form.get('dropdown')


    dataset=repo['lmy1031_zhuoshu.position'].find()
    y   = []

    for i in dataset:
        y.append(i)

    dataset=y[0]
        #delete useless key
    dataset.pop('_id', None)
    a=dataset["location"]
    print(type(a))

    return render_template('proj3.html', message = a)
    


if __name__ == '__main__':
    app.run()
