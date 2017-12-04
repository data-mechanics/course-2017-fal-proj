import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import dml
import sys
import numpy as np
import folium
import folium.plugins as plg
from collections import OrderedDict


def parse_data():
    '''Sets up the connect to MongoDB to read in our
    crime statistics. Builds and returns a dataset of the crimes each month in
    Boston.'''

    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('esaracin', 'esaracin')

    dataset = repo['esaracin.crime_incidents'].find()
    df_crime = pd.DataFrame(list(dataset))

    # Extract the year-month of each crime in Boston
    crimes_by_month = {}
    for ind, row in df_crime.iterrows():
        year_month = row['OCCURRED_ON_DATE'][:7]

        if eval(row['Location']) == (0, 0) or eval(row['Location']) == (-1, -1):
            continue

        if year_month in crimes_by_month:
            crimes_by_month[year_month].append(eval(row['Location']))
        else:
            crimes_by_month[year_month] = [eval(row['Location'])]


    # Delete the last month, as it has considerably fewer data objects to work
    # with
    crimes_by_month = OrderedDict(sorted(crimes_by_month.items(), key=lambda t: t[0]))
    del crimes_by_month['2017-11']

    return crimes_by_month


def build_map(data, timeseries):
    '''Uses folium to build the make using the passes in data. Saves the map to
    a local .html file.'''

    m = folium.Map([42.3601, -71.0589], tiles='stamentoner', zoom_start=12)
    hm = plg.HeatMapWithTime(data, index=timeseries)
    hm.add_to(m)
    m.save('heatmap.html')


def main():
    crimes_by_month = parse_data()


    # Construct a larger dataset containing each of the 29 months investigated
    # to be passed in to build our map
    data = []
    for key in crimes_by_month:
        crimes = crimes_by_month[key]
        month = []
        for lat, lon in crimes:
            month.append([lat, lon])

        data.append(month)


    # Build a timeseries to accompany the data
    first_date = datetime(2015, 6, 1, 00, 00)
    timeseries = [first_date.strftime('%Y-%m-%d')]
    for i in range(1, len(data)):
        first_date = first_date + relativedelta(months=+1)
        to_append = first_date.strftime('%Y-%m-%d')

        timeseries.append(to_append)


    build_map(data, timeseries)



if __name__ == '__main__':
    main()





#list_lens = []
#for key in crimes_by_month:
#    list_lens.append(len(crimes_by_month[key]))

#min_len = min(list_lens)

#for key in crimes_by_month:
#    crimes_by_month[key] = crimes_by_month[key][:min_len]



