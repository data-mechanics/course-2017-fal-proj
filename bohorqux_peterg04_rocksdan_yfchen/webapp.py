import jsonschema
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask.ext.httpauth import HTTPBasicAuth
from pymongo import MongoClient
from _overlapped import NULL
from geopy.geocoders import Nominatim
import math


# Set up the database connection.
client = MongoClient()
repo = client.repo
repo.authenticate('bohorqux_peterg04_rocksdan_yfchen', 'bohorqux_peterg04_rocksdan_yfchen')

geolocator = Nominatim()
# restaurants = repo['bohorqux_peterg04_rocksdan_yfchen.Restaurants']

app = Flask(__name__)
auth = HTTPBasicAuth()

users = [
  { 'id': 1, 'username': u'yfchen' },
  { 'id': 2, 'username': u'bohorqux' },
  { 'id': 3, 'username': u'rocksdan' },
  { 'id': 4, 'username': u'peterg04' },
  { 'id': 5, 'username': u'lapets' }
]

schema = {
  "type": "object", 
  "properties": {"username" : {"type": "string"}},
  "required": ["username"]
}

def dist(p, q):
        (x1,y1) = p
        (x2,y2) = q
        return math.sqrt((x1-x2)**2 + (y1-y2)**2)
    
@app.route('/', methods=['OPTIONS'])
def show_api():
    return jsonify(schema)

@app.route('/', methods=['GET', 'POST'])
@auth.login_required
def client():
    print("rendering index")
    collection = repo['bohorqux_peterg04_rocksdan_yfchen.kmeans'].find_one()
    
    smallest = list()
    smallest.append((collection["center1"]["radius"], 1))
    smallest.append((collection["center2"]["radius"], 2))
    smallest.append((collection["center3"]["radius"], 3))
    smallest.append((collection["center4"]["radius"], 4))
    smallest.append((collection["center5"]["radius"], 5))
    
    smallest_radii = list()
    while len(smallest_radii) < 2:
         smallest_radii.append(min(smallest))
         smallest.remove(min(smallest))
         
    # this part is for the find location route
    whole = None
    success = None
    
    return render_template("index.html", radii=smallest_radii, collection=collection, whole=whole, success=success)

@app.route('/findLocation', methods=["GET", "POST"])
@auth.login_required
def findLocation():
    collection = repo['bohorqux_peterg04_rocksdan_yfchen.kmeans'].find_one()
    
    smallest = list()
    smallest.append((collection["center1"]["radius"], 1))
    smallest.append((collection["center2"]["radius"], 2))
    smallest.append((collection["center3"]["radius"], 3))
    smallest.append((collection["center4"]["radius"], 4))
    smallest.append((collection["center5"]["radius"], 5))
    
    smallest_radii = list()
    while len(smallest_radii) < 2:
         smallest_radii.append(min(smallest))
         smallest.remove(min(smallest))
         
         
    if request.method == "POST":
        street = request.form.get('street')
        city = request.form.get('city')
        state = request.form.get('state')
        zipcode = request.form.get('zipcode')
        country = request.form.get('country')

        address, (latitude, longitude) = geolocator.geocode(street + " " + city + " " + state)
        
        whole = [address, latitude, longitude]
    else:
        whole = None
    
    
    success = None
    if whole != None:
        # check for whether the coordinates are within radii distance
        center1 = (collection["center" + str(smallest_radii[0][1])]["centroid"][0], collection["center" + str(smallest_radii[0][1])]["centroid"][1])
        center2 = (collection["center" + str(smallest_radii[1][1])]["centroid"][0], collection["center" + str(smallest_radii[1][1])]["centroid"][1])
        if dist((longitude,latitude), center1) <= smallest_radii[0][0] or dist((longitude,latitude), center2) <= smallest_radii[1][0]:
            distance = min([dist((longitude,latitude), center1), dist((longitude,latitude), center2)])
            success = True
        else:
            distance = min([dist((longitude,latitude), center1), dist((longitude,latitude), center2)])
            success = False
    
    return render_template("index.html", radii=smallest_radii, collection=collection, whole=whole, success=success, distance=distance)

@app.route('/app/api/v0.1/users', methods=['GET'])
def get_users(): # Server-side reusable name for function.
    print("I'm responding.")
    return jsonify({'users': users})

@app.route('/app/api/v0.1/users/', methods=['GET'])
def get_user(user_id):
    user = [user for user in users if user['id'] == user_id]
    if len(user) == 0:
        abort(404)
    return jsonify({'user': user[0]})

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found foo.'}), 404)

@app.route('/app/api/v0.1/users', methods=['POST'])
def create_user():
    print(request.json)
    if not request.json:
        print('Request not valid JSON.')
        abort(400)

    try:
        jsonschema.validate(request.json, schema)
        user = { 'id': users[-1]['id'] + 1, 'username': request.json['username'] }
        users.append(user)
        print(users)
        return jsonify({'user': user}), 201
    except:
        print('Request does not follow schema.')
        abort(400)

@auth.get_password
def foo(username):
    if username == 'alice':
        return 'ecila'
    return None

@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access.'}), 401)

if __name__ == '__main__':
    app.run(debug=True)
    