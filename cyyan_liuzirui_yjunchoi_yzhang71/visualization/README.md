# Project 3 Visualization
## Required libraries and tools
First, you will need some libraries and packages. By downloading through pip, you can easily install the latest versions.
```
python -m pip install flask
python -m pip install json
python -m pip install pymongo
python -m pip install time
```
Next, obtain a mapbox access token for leaflet.js. [Link](https://www.mapbox.com)
And put in `home.html` (Line 126) and `map.html` (Line 132).
```
accessToken: 'your.mapbox.access.token'
```
Lastly, run main.py and visit `127.0.0.1:5000`
```
python main.py
```
