from flask import Flask
from flask import Flask, render_template, request, abort, redirect, url_for, send_from_directory, flash
from newStations import newStations
import json

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('indexkmeans.html')


@app.route("/ChooseK/", methods=["POST", "GET"])
def processinput():
    try:
        k = int(request.form["k"])
        if k <= 0:
            flash('Please enter a number above 0')
            return redirect(url_for('index'))
        elif k > 15:
            flash('Please enter a number under or equal to 15')
            return redirect(url_for('index'))
        else:
            result = newStations(k)
            answer = newStations.execute(result)
            print(answer)
            return render_template('policemap.html', result = json.dumps(answer), clusters = k)

    except Exception as e:
        flash('Please enter a number')
        return redirect(url_for('index'))




if __name__ == '__main__':

    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(debug=True)