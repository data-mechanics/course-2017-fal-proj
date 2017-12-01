from flask import Flask, render_template, request

app = Flask(__name__)


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/means')
def data():
    # here we want to get the value of user (i.e. ?user=some-value)
    means = int(request.args.get('means'))
    if means > 0:
        return "You selected {} means".format(means)
    else:
        return "Please enter a valid number of means"


if __name__ == "__main__":
    app.run()
