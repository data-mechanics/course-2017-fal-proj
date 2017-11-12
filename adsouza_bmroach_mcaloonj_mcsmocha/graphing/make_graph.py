"""
Filename: make_graph.py

Last edited by: BMR 11/11/17

Boston University CS591 Data Mechanics Fall 2017 - Project 2
Team Members:
Adriana D'Souza     adsouza@bu.edu
Brian Roach         bmroach@bu.edu
Jessica McAloon     mcaloonj@bu.edu
Monica Chiu         mcsmocha@bu.edu

Development Notes: 
- still in development (not functional)

"""

import geojson
from geoql import geoql
import geoleaflet
import dml

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('adsouza_bmroach_mcaloonj_mcsmocha', 'adsouza_bmroach_mcaloonj_mcsmocha')
points = repo['adsouza_bmroach_mcaloonj_mcsmocha.signal_placements'].find()
repo.logout()

print(points[0])

"""
#url = 'https://raw.githubusercontent.com/Data-Mechanics/geoql/master/examples/'

# Boston ZIP Codes regions.
#z = geoql.loads(requests.get(url + 'example_zips.geojson').text, encoding="latin-1")

# Extract of street data.
g = geoql.loads(requests.get(url + 'example_extract.geojson').text, encoding="latin-1")
g = geoql.loads(requests.get('http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson').text, encoding="latin-1")


# g = g.properties_null_remove()\
#      .tags_parse_str_to_dict()\
#      .keep_by_property({"highway": {"$in": ["residential", "secondary", "tertiary"]}})
# g = g.keep_within_radius((42.3551, -71.0656), 4, 'miles') # 0.75 miles from Boston Common.
# g = g.keep_that_intersect(z) # Only those entries found in a Boston ZIP Code regions.


g = g.node_edge_graph() # Converted into a graph with nodes and edges.
g.dump(open('example_extract.geojson', 'w'))
open('placements.html', 'w').write(geoleaflet.html(g)) # Create visualization.
"""