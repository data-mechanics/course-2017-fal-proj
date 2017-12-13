from flask import Flask, render_template, request

import find_kmeans
import json

app = Flask(__name__)


@app.route('/')
def main():
    return render_template('index.html')

@app.route('/kmeans', methods=["GET","POST"])
def run_kmeans():
    if request.method == "POST":
        try:
            distance=float(request.form['distance'])
            toggle = request.form['option']
            kmeanFinder=find_kmeans.FindKMeans(distance)
            if toggle == 'average':
                results=kmeanFinder.execute(toggle = True)
            else:
                results = kmeanFinder.execute(toggle=False)
        except:
            return render_template('index.html',msg = 'Invalid Input')


    else:
        return render_template('index.html')


    return render_template('results.html',results = results,type = toggle,distance = distance)
if __name__ == "__main__":
    app.run()
    