# CS 591 Fall 2017 Project #2: Modeling, Optimization, and Statistical Analysis
Jake Bloomfeld (jtbloom@bu.edu), Ricardo Ballesteros (rfballes@bu.edu), Daniel Medina (medinad@bu.edu)

#### Abstract
Hubway is a public bike-share system serving the people of Boston, Brookline, Cambridge, and Somerville. With roughly 1800 bikes in practically 200 stations, the Hubway serves as a fun, affordable, and convenient transportation option for quick trips around Boston and its surrounding municipalities. The objective of our project stemmed from this question: is there a correlation between income per capita in Boston's neighbrohoods with the number of Hubway trips taken in that neighborhood? Seeing if such a correltation exists can pose further questions for discussion, such as, are there more trips being taken to/from wealthier neighborhoods than poorer neighborhoods? Are wealthier/poorer neighborhoods over/underutilizing available Hubway bikes? Our project tries to solve this problem.

### Datasets
* Hubway Station Locations: 'https://boston.opendatasoft.com/explore/dataset/hubway-station-locations/download/?format=geojson&timezone=America/New_York'
* Hubway Trip History: 'http://datamechanics.io/data/jb_rfb_dm_proj2data/201708_hubway_tripdata2.json'
* Per Capita Income by Neighborhood: 'http://datamechanics.io/data/jb_rfb_dm_proj2data/incomeByNeighborhood.json'




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
 
 
