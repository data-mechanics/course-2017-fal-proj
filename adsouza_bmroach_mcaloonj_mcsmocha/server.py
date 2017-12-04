"""
Filename: server.py

A flask web server to call our algorithm with user-defined parameters and return
resulting metrics and graphs

Last edited by: BMR 12/2/17

Boston University CS591 Data Mechanics Fall 2017 - Project 3
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu


Development Notes:

"""
from flask import Flask, render_template, request, url_for, jsonify
from threading import Thread
from logic import algo
app = Flask(__name__)
th = Thread()

finished = False
params = {}
requestCount = 1

@app.route('/', methods=['GET'])
def main():
    return render_template('index.html')

@app.route('/placeholder')
def placeholder():
    return render_template('placeholder.html')


@app.route('/getmap', methods=['POST'])
def getmap():
    global finished
    finished = False
    global requestCount
    requestCount += 1
    ms = float(request.form['Mean Skew'])
    r = float(request.form['Radius'])
    cd = int(request.form['Cluster Divisors'])
    sc = int(request.form['Sign Count'])
    bs = float(request.form['Buffer Size'])

    global params
    params = {'mean_skew': ms, #default 1.0
              'radius': r, #default 2
              'cluster_divisor': cd, #default 15
              'sign_count': sc, #default 30
              'buffer_size': bs, #default .5
            }
        
    try: 
        global th
        th = Thread(target=worker, args=())
        th.start()                        
        return render_template('loading.html')
    except:        
        return render_template('error.html')

def worker():
    algo(params, requestCount)
    global finished
    finished = True
    return

@app.route('/thread_status')
def thread_status():
    return jsonify(dict(status=('finished' if finished else 'running')))

@app.route('/result')
def result():
    return render_template('placements.html')


if __name__ == '__main__':
    app.run()