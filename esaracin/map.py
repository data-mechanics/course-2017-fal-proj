#from folium import *
import folium
from folium.plugins import time_slider_choropleth
import pandas as pd
import numpy as np
import dml
import sys
import json
import os


map_obj = folium.Map(location=[42.3601, -71.0589], zoom_start=12, tiles='Stamen Terrain')

styledict = {str(elem): {} for elem in range(1, 21)}
for key in styledict:
    for tag in range(1, 21):
        if tag <= int(key):
            styledict[key][str(tag)] = {'color': 'ffffff', 'opacity': 1}
        else:
            styledict[key][str(tag)] = {'color':'ffffff', 'opacity': 0}


geoJson = json.dumps([{"coordinates": [42.3224598923, -71.0827607325], "type": "Point", "id": "1"}, 
           {"coordinates": [42.31978474, -71.0997123034], "type": "Point", "id": "2"}, 
           {"coordinates": [42.3115315764, -71.0747026509], "type": "Point", "id": "3"},
           {"coordinates": [42.2775818651, -71.1551150717], "type": "Point", "id": "4"}, 
           {"coordinates": [42.285049948, -71.1228471399], "type": "Point", "id": "5"}, 
           {"coordinates": [42.2849752262, -71.1231301265], "type": "Point", "id": "6"},
           {"coordinates": [42.304725376, -71.0809026798], "type": "Point", "id": "7"}, 
           {"coordinates": [42.3282664806, -71.0786577989], "type": "Point", "id": "8"}, 
           {"coordinates": [42.3135237403, -71.0632472203], "type": "Point", "id": "9"}, 
           {"coordinates": [42.3165566118, -71.1026277], "type": "Point", "id": "10"},
           {"coordinates": [42.3870617571, -71.0098932771], "type": "Point", "id": "11"}, 
           {"coordinates": [42.3020314658, -71.0985437465], "type": "Point", "id": "12"}, 
           {"coordinates": [42.3462751604, -71.0773171194], "type": "Point", "id": "13"}, 
           {"coordinates": [42.3079955185, -71.1294575148], "type": "Point", "id": "14"},
           {"coordinates": [42.3397892259, -71.1204616282], "type": "Point", "id": "15"}, 
           {"coordinates": [42.3505924802, -71.142917872], "type": "Point", "id": "16"}, 
           {"coordinates": [42.3717397741, -71.0461999197], "type": "Point", "id": "17"},
           {"coordinates": [42.3049946436, -71.0668733176], "type": "Point", "id": "18"},
           {"coordinates": [42.3526188719, -71.0580945445], "type": "Point", "id": "19"},
           {"coordinates": [42.3776105431, -71.0305478316], "type": "Point", "id": "20"}])


g = time_slider_choropleth.TimeSliderChoropleth(geoJson, styledict=styledict,).add_to(map_obj)

print('here')

#map_obj.save(os.path.join('.', 'TimeSliderChoropleth.html'))
map_obj.save('cluster_map.html')
