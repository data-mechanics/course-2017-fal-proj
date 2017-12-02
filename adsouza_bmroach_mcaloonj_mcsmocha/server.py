"""
Filename: server.py

A flask web server to call our algorithm with user-defined parameters and return
resulting metrics and graphs

Last edited by: BMR 11/28/17

Boston University CS591 Data Mechanics Fall 2017 - Project 3
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu


Development Notes:

"""

from flask import Flask, render_template, request, url_for
from logic import algo
app = Flask(__name__)

@app.route('/', methods=['GET'])
def main():
    return render_template('index.html')


@app.route('/algo', methods=['POST'])
def algo():
    ms = float(request.form['Mean Skew'])
    r = float(request.form['Radius'])
    cd = float(request.form['Cluster Divisors'])
    sc = float(request.form['Sign Count'])
    bs = float(request.form['Buffer Size'])

    params = {'mean_skew': ms, #default 1.0
              'radius': r, #default 2
              'cluster_divisor': cd, #default 15
              'sign_count': sc, #default 30
              'buffer_size': bs, #default .5
            }

    
    try: 
        algo(params)
        #placements.html is generated from make_graph
        return render_template('placements.html')
    except:
        return render_template('error.html')



if __name__ == '__main__':
    app.run()