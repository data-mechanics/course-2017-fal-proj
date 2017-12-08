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


remote_server = True
finished = {}

params = {}
requestCount = 5
call_id = 0

@app.route('/', methods=['GET'])
def main():
    return render_template('index.html')

@app.route('/placeholder')
def placeholder():
    return render_template('placeholder.html')


@app.route('/getmap', methods=['POST'])
def getmap():

    global finished
    global call_id
    finished[call_id] = False
    this_call = call_id
    call_id += 1
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
        th = Thread(target=worker, args=[this_call])
        th.start()                        
        return render_template('loading.html', tID=str(this_call))
    except:        
        return render_template('error.html')

def worker(*args):
    this_call = args[0]
    algo(params, requestCount, this_call)
    global finished
    finished[this_call] = True
    return

@app.route('/thread_status/<int:a>')
def thread_status(a):
    thread_id = int(a)
    return jsonify(dict(status=('finished' if finished[thread_id] else 'running')))

@app.route('/result/<int:a>')
def result(a):
    thread_id = str(a)
    return render_template('placements'+thread_id+'.html')


if __name__ == '__main__':
    if remote_server:
        # app.run(threaded=True, host='0.0.0.0', port='80')
        algo(params,5,0,just_fetch=True)
        from gevent.wsgi import WSGIServer
        http_server = WSGIServer(('0.0.0.0',80),app)
        http_server.serve_forever()
    else:
        app.run()