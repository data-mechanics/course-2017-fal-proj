import folium
import dml
import folium.plugins as plg
import pandas as pd
from collections import OrderedDict
import sys


def main():
    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('esaracin', 'esaracin')

    dataset = repo['esaracin.crime_incident_centers_iterative'].find()
    df_crime = pd.DataFrame(list(dataset))
    
    centers = {}
    for col in df_crime:
        centers[col] = df_crime[col]

    del centers['_id']

    # Parse the data into a form usable by Folium
    data = []
    for i in range(1, len(centers) + 1):
        curr_centers = centers[str(i)]
        data.append(curr_centers[0])

    # Create our "timeseries," which for this map will simply correspond to the
    # number of clusters used in each fram
    timeseries = []
    for i in range(1, len(data) + 1):
        curr_str = 'The ' + str(i) + ' Cluster Approach'
        timeseries.append(curr_str)

    # Create the heatmap with the timeseries above
    m = folium.Map([42.3601, -71.0589], tiles='stamentoner', zoom_start=12)
    hm = plg.HeatMapWithTime(data, index=timeseries)
    hm.add_to(m)

    # Add lat/long popup information for percise location of where the
    # crime-centers are.
    folium.LatLngPopup().add_to(m)

    m.save('crime_centers.html')


if __name__ == '__main__':
    main()
