import flask
from flask import Flask, render_template
import pymongo


app = Flask(__name__)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('cyyan_liuzirui_yjunchoi_yzhang71', 'cyyan_liuzirui_yjunchoi_yzhang71')

@app.route('/',methods=['GET'])
def hello():
    return render_template('index.html')

@app.route('/',methods=['POST'])
def register_user():
	try:
		wardname = request.form.get('ward')
		drop =request.form.get('dropdown')
		print("ok")
		return render_template('index.html')
	except:
		print("couldn't find all tokens")
		return render_template('index.html')

@app.route('/score')
def score():
    return render_template('score.html')

if __name__ == '__main__':
    app.run()