from flask import Flask, render_template, request

app = Flask(__name__)


@app.route("/")
def index():
    return render_template('index.html')

@app.route('/visualization')
def visualization():
    # here we want to get the value of user (i.e. ?means=some-value)
    try:
        routes = int(request.args.get('routes'))
        means = int(request.args.get('means'))
    except ValueError:
        return "Please enter a valid integer input"
    if routes > 0 and means > 0:
        return "You selected {} means and {} routes".format(means, routes)
    else:
        return "Please enter a valid number of means/routes"


if __name__ == "__main__":
    app.run()
