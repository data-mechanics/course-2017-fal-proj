# Authors: Ben Gaudiosi, Ray Katz, Ned Gleesin
import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo
import flask.ext.login as flask_login

app = Flask(__name__)
mongo = PyMongo(app)
app.secret_key = 'isok'

# index page
@app.route("/", methods=['GET'])
def welcome():
    return render_template('index.html')

if __name__ == "__main__":
    # this is invoked when in the shell  you run
    # $ python app.py
    app.run(port=5000, debug=True)
