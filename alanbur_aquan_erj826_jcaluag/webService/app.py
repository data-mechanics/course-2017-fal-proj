from flask import Flask, render_template, request
import os
import find_kmeans
import json

template_dir = os.path.abspath('static')
app = Flask(__name__,template_folder=template_dir)

@app.route('/')
def main():
    return render_template('index.html')

@app.route('/kmeans', methods=["POST"])
def run_kmeans():
    distance=float(request.form['distance'])
    kmeanFinder=find_kmeans.FindKMeans(distance)
    results=kmeanFinder.execute()
    resultsJson=json.dumps(results)
    return(resultsJson)
if __name__ == "__main__":
    app.run()
    