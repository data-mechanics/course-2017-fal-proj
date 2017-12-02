from flask import Flask, render_template, request

app = Flask(__name__)


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/means')
def means():
    # here we want to get the value of user (i.e. ?means=some-value)
    means = int(request.args.get('means'))
    if means > 0:
        return "You selected {} means".format(means)
    else:
        return "Please enter a valid number of means"


@app.route('/emergency_routes')
def routes():
    # here we want to get the value of user (i.e. ?means=some-value)
    routes = int(request.args.get('routes'))
    if routes > 0:
        return "You selected {} routes".format(routes)
    else:
        return "Please enter a valid number of routes"


if __name__ == "__main__":
    app.run()
