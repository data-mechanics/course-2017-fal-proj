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

finished = {}

app = Flask(__name__)
th = Thread()

requestCount = 1
call_id = 1

@app.route('/', methods=['GET'])
def main():
    return render_template('index.html')

@app.route('/placeholder')
def placeholder():
    return render_template('placeholder.html')


@app.route('/getmap', methods=['POST'])
def getmap():
    ms = float(request.form['Mean Skew'])
    r = float(request.form['Radius'])
    cd = int(request.form['Cluster Divisors'])
    sc = int(request.form['Sign Count'])
    bs = float(request.form['Buffer Size'])

    params = {'mean_skew': ms, #default 1.0
              'radius': r, #default 2
              'cluster_divisor': cd, #default 15
              'sign_count': sc, #default 30
              'buffer_size': bs, #default .5
            }

    global requestCount
    requestCount += 1

    global finished
    #keeps track of calls with identical parameters and returns them from the cached responses
    completed_requests = [(key, val) for (key, val) in finished.items() if val[0]==True]
    cache_hit = False
    for call in completed_requests:
        if call[1][1] == params:
            this_call = call[0]
            cache_hit = True
            break

    if not cache_hit:
        global call_id
        this_call = call_id
        call_id += 1

        finished[this_call] = [False, params]    

    try:
        if not cache_hit: 
            global th
            worker_params = params
            th = Thread(target=worker, args=[this_call, worker_params])
            th.start()                        
        return render_template('loading.html', tID=str(this_call))
    except: 
        return render_template('error.html')
        

def worker(*args):
    this_call = args[0]
    worker_params = args[1]
    algo(worker_params, requestCount, this_call)
    global finished
    finished[this_call][0] = True
    return

@app.route('/thread_status/<int:a>')
def thread_status(a):
    thread_id = int(a)
    return jsonify(dict(status=('finished' if finished[thread_id][0] else 'running')))

@app.route('/result/<int:a>')
def result(a):
    thread_id = str(a)
    return render_template('placements'+thread_id+'.html')


if __name__ == '__main__':
    app.run()