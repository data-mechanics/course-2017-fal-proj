from flask import Flask
from flask import request
from flask import render_template
app = Flask(__name__)
from gettingdata import gettingdata
import json

app = Flask(__name__)
app.debug = True
# @app.route('/')
# def index():
# 	return render_template('index.html')

@app.route('/')
def index():
	
	data = gettingdata.get_data()
	print(data)
	return render_template('index.html',data=data)

@app.route('/map')
def map():
	return render_template('map.html')


if __name__ == '__main__':
    app.run()