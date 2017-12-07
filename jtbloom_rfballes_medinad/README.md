# CS 591 Fall 2017 Final Project
### Boston Hubway Trips in Relation to Average Neighborhood Income and Population 
Jake Bloomfeld (jtbloom@bu.edu), Ricardo Ballesteros (rfballes@bu.edu), Daniel Medina (medinad@bu.edu)

#### Introduction
Hubway is a public bike-share system serving the people of Boston, Brookline, Cambridge, and Somerville. With roughly 1800 bikes in practically 200 stations, the Hubway serves as a fun, affordable, and convenient transportation option for quick trips around Boston and its surrounding municipalities. In 2016 alone, over 100,000 people took roughly 1.2 million trips totaling just over 2.8 million miles. As sustainable transportation methods are becoming more popular in cities, we had questions of whether or not a relationship exists with the frequency of trips within neighborhoods and that neighborhood's average income. With some further thought, we proposed our final questions: is there a correlation between income per capita in Boston's neighborhoods with the number of Hubway trips taken in that neighborhood? Also, is there a correlation between neighborhood population with Hubway trips taken in that neighborhood? Seeing if such relationships exists can pose further, more interesting points for discussion and analysis. We would be able to examine whether more trips are being taken to and from wealthier neighborhoods than poorer neighborhoods. This could be useful in seeing if socioeconomic conditions of a nerighborhood has any effect on the frequency of trips to and from that neighborhood. Also, we would be able to examine whether neighborhoods with a higher population density have a high or low frequency of trips, potentially raising the concern of over/underutilizing bikes in that specific neighborhood. These are the questions that we plan to tackle in our project.

#### Data Sets
To solve this problem and perform the necessary analysis, we use and manipulate several datasets as described below:
<br><br>
*Hubway Station Locations* <br>
* Description: This dataset consists of the name of each Hubway station, its longitude and latitude coordinates, number of bike docks, and the municipality it resides in.
* Source: Analyze Boston.
<br><br>
*Hubway Trip History* <br>
* Description: This dataset consists of trip histories of a given month, including the trip duration (in seconds), start time and date, stop time and date, start station name & ID, end station name & ID, bike ID, user type (Casual = 24-Hour or 72-Hour Pass user; Member = Annual or Monthly Member), and zip code (if member). 
* Source: Hubway System Data.
<br><br>
*Per Capita Income by Boston Neighborhood* <br>
* Description: This dataset consists of each Boston neighborhood with its average income per capita. 
* Source: Census Bureau
<br><br>
*Population by Boston Neighborhood* <br>
* Description: This dataset consists of populations by Boston neighborhoods.
* Source: Census Bureau
<br><br>
*Boston Neighborhoods* <br>
* Description: This dataset GeoJSON polygons, which are enclosed areas within certain geographical coordinates that represents the different neighborhoods in Boston.
* Source: Boston OpenDataSoft
<br><br>

#### Methods
Once we gathered the datasets, we had to perform some transformations on them in order to get them into the state that would be easiest for us to work with. Due to the fact that we only had average per capita income and population by Boston neighborhood, we had to filter out all of the Hubway stations in the Hubway Station Locations dataset that were within the Boston municipality (i.e.Brookline, Cambridge, Somerville, etc.). For the Hubway Trip History dataset, we filtered out trip durations, start/stop times and dates, bike IDs, user type and zip code. Our main use for this dataset was to calculate the frequency of trips in each station, so we didn't have any need for extraneous information. For the remaining three datasets, we did not have to manipulate much as the original data was already in good shape to use.
<br><br>
The first step we had to take was to figure out which Hubway stations fall in which neighborhood in Boston. In the Hubway Station Locations dataset, we were given station names and geographical coordinates, so we had to map the coordinates to a specific Boston neighborhood. Given the coordinate points of the station, we checked if the station is in certain Boston neighborhood, given by the Boston Neighborhood Geoshape dataset, and added a neighborhood attribute to each Hubway statio. Once all Hubway stations were mapped to a certain Boston neighborhood, we were then able to use the Average Per Capita Income and Average Population by Neighborhood datasets.

#### Results
In order to perform statistical analysis, we used the Pearson product-moment correlation coefficient to determine the strength and direction of the linear relationship between the number of Hubway trips per neighborhood with its average income and population. Using a sample of trips taken in one month, we were able to obtain the following results: <br><br>

Correlation between # of Hubway Trips and Average Per-Capita Income by Neighborhood: <br>
Correlation Coefficient: -0.376  
P-value: 0.133
<br>
<br>
Correlation between # of Hubway Trips and Average Population by Neighborhood: <br>
Correlation Coefficient: 0.377
P-value: 1
<br>
<br>

#### Conclusion
Based on our calculations there is a moderate negative correlation between the number of Hubway trips per neighborhood and average neighborhood income. There is also a moderate positive correlation with the average neighborhood population with a high p-value, meaning that this observation is a non-significant result. Based on these results, we can learn that neighborhoods with higher frequencies of trips tend to have a lower average per-capita income. 

#### Future Work
For expanding on this project in the future, there are several aspects to consider:
Combining Hubway trip history data sets from past several years to get an even more accurate correlation data
Using datasets of income per capita and population in municipalities outside of Boston to get a bigger picture










# CS 591 Fall 2017 Project #2: Modeling, Optimization, and Statistical Analysis
Jake Bloomfeld (jtbloom@bu.edu), Ricardo Ballesteros (rfballes@bu.edu), Daniel Medina (medinad@bu.edu)

### Narrative
Hubway is a public bike-share system serving the people of Boston, Brookline, Cambridge, and Somerville. With roughly 1800 bikes in practically 200 stations, the Hubway serves as a fun, affordable, and convenient transportation option for quick trips around Boston and its surrounding municipalities. The objective of our project stemmed from this question: is there a correlation between income per capita in Boston's neighbrohoods with the number of Hubway trips taken in that neighborhood? What about neighborhood population with Hubway trips? To solve this problem and perform the necessary analysis, we use and manipulate several datasets. In the Hubway Trip History dataset, given the coordinate points of the station, we check if the station is within a certain Boston neighborhood, given by the Boston Neighborhood Geoshape dataset. Then, once the Hubway stations are mapped to a certain neighborhood, using the Per Capita Income and Population by Neighborhood dataset, we see if there is a correlation between these three dimensions: incoming/outgoing Hubway trips per neighborhood, average per capita income by neighborhood, and population by neighborghood. Seeing if such a correlatation exists can pose further questions for discussion, such as, are there more trips being taken to/from wealthier neighborhoods than poorer neighborhoods? Are wealthier/poorer neighborhoods or higher/lower population neighborhoods over/underutilizing available Hubway bikes? Our project tries to solve this problem.

### Datasets
* Hubway Station Locations: 'https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/download/?format=geojson&timezone=America/New_York'
* Hubway Trip History: 'http://datamechanics.io/data/jb_rfb_dm_proj2data/201708_hubway_tripdata2.json'
* Per Capita Income by Neighborhood: 'http://datamechanics.io/data/jb_rfb_dm_proj2data/incomeByNeighborhood.json'
* Boston Neighborhood Geoshapes: 'http://datamechanics.io/data/jb_rfb_dm_proj2data/bos_neighborhoods_shapes.json'

### Scripts
* stationsByneighborhood.py: this script was used to find out what Hubway stations fall in what neighborhood in the boston area. 
* neighborhood_station_income_out.py: was used to aggregate the number of trips that come out of each neighbprhood in order to execute the statistical analyses on (trips, income)
* trips_income_correlation.py: executes a correation between the neighbprhood income and trips. Our findings give us a positive correlations between the vectors. 
* trips_population_correlation.py: 
* neighborhood_income.py: was used to transform the data from out income per capita by neighborhood data set. Was used to aggregate trips and to again execute a correlation. This time between the population and trips. Our findings suggest a negative correlation. This might be intersting to further investogate in project 3 and see what is it that affects this number. 

### note
Be sure to run get_datasets.py before anything else.
In order to run the two correlation files newbosneighborhoods.py, outgoing_trips.py, neighborhood_income.py, neighborhood_station_income_out.py must be run beforehand. 


# CS 591 Fall 2017 Project #1: Data Retrieval, Storage, Provenance, and Transformations 
Jake Bloomfeld (jtbloom@bu.edu) and Ricardo Ballesteros (rfballes@bu.edu)
## Project Idea: Sustainability and Transportation in Boston

### Notes
* We didn't use any APIs
* At the end of each transformation script, we commented out ''class'.execute()'
* At the end of get_datasets.py, we commented out the last 4 lines

### Dataset #1: Electric Vehicle Charging Stations (Mass DOT)
##### Website URL: http://geo-massdot.opendata.arcgis.com/datasets/electric-vehicle-charging-stations/data
##### JSON URL: https://opendata.arcgis.com/datasets/ed1c6fb748a646ac83b210985e1069b5_0.geojson
* Important info: station name, address, longitude and latitude, city
  
### Dataset #2: Hubway Station Locations (Boston OpenDataSoft)
#### Website URL: https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/
#### JSON URL: https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/download/?format=geojson&timezone=America/New_York
* Important info: station name, longitude and latitude, municipality, # of docks

### Dataset #3: Existing Bike Network (Analyze Boston)
#### Website URL: https://data.boston.gov/dataset/existing-bike-network
#### JSON URL: http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.geojson
* Important info: geo point, geo shape, street name
  
### Dataset #4: Boston Neighborhoods (Boston OpenDataSoft)
#### Website URL: https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/
#### JSON URL: https://boston.opendatasoft.com/explore/dataset/boston-neighborhoods/download/?format=geojson&timezone=America/New_York
* Important info: geo point, geo shape, acres, square miles, neighborhood #

### Dataset #5: Hubway Trip History (Hubway)
#### Website URL: https://www.thehubway.com/system-data
#### JSON URL: http://datamechanics.io/data/jt_rf_pr1/hubway_trip_history.json
* Important info: start station name, start station coordinates, end station name, end station coordinates, start time, stop time

### Narrative
For Project  #1, we picked data sets that revolved around a common theme: sustainability and transportation in Boston. Although at this state we don’t know what specific problem we want to solve, we went ahead to search for data sets that could potentially lead us in the right direction. The data sets we chose are:

* Electrical Vehicle Charging Stations 
* Hubway Station Locations
* Existing Bike Network
* Boston Neighborhoods
* Hubway Trip History

The Electrical Vehicle Charging Stations dataset gives us the names of the stations along with their addresses and geographical coordinate points. The Hubway Station Locations dataset is pretty similar, as it also gives us the names of the stations, their addresses, geographical coordinate points, along with the number of bike docks. The Bike Network dataset gives us a visual representation of bike paths in Boston. Likewise, the Boston Neighborhoods dataset gives usa visual representation of the different neighborhoods within Boston, which could be used to filter data based on neighborhood. Lastly, the Hubway Trip History dataset includes information about trip starting and ending locations, when the trip occured, and duration. We believe that with the right tools, algorithms, and creativity, these datasets can be combined to create a very interesting and informative project.

### Transformations
#### Transformation 1:
For the first transformation, we wanted to modify and clean up the Electric Vehicle Charging Station dataset. This dataset gives us all electric vehicle charging stations within the whole state of MA, but for the purpose of this project, we wanted to narrow them down to only include the stations in Boston. We performed a selection to filter the data, thus, retreiving the stations where the city was equal to 'Boston'. Once the dataset was narrowed down to just Boston stations, we wanted to clean up the dataset and remove any data that we thought was extraneous. The only other fields we wanted to include were 'Station Name', 'Address', 'Longtitude', and 'Latitude'. After extracting those fields, we inserted the new dataset into a dictionary, which was then inserted into a new MongoDB collection.

Transformation file: boston_charging_stations.py

#### Transformations 2 & 3:
These two trasformations are similar in that they are both the derived from the Hubway Trip History. The final results tell us the number of (1)incoming trips to every Hubway station in the month of January 2015 and (2) outgoing trips of every hubway station in the month of January 2015. The results were obtained by selecting, projecting into a tuple list, and then aggregating by counting the number of bikes that either started or finished a trip at a certain station. When continuing further in this project, we can combine trip history from many different months and years to get a larger set of data.

Transformation files: outgoing_trips.py, incoming_trips.py

#### Transformation 4:
This transformation modifies the Hubway Station Locations dataset to see how many Hubway docks are in each municipality within Boston. First, we do a selection to filter the dataset. The fields we select are the municipality of the station within the city of Boston, the station name, the number of bike docks, and the longitude and latitude. Then, we projected the municipality and the number of docks into a tuple list. Lastly, we aggregated by sum so the final result shows us how many docks there are per municipality.

Transformation files: boston_hubway_stations.py
 
 
