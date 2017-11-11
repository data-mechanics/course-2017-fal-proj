## Data set:
Data set for Wards: https://data.boston.gov/dataset/wards

Data set for Precincts: https://data.boston.gov/dataset/precincts

Data set for Polling Location: https://data.boston.gov/dataset/polling-locations

Data set for Boston Population: https://www.opendatanetwork.com/entity/1600000US2507000/Boston_MA/demographics.population.count?year=2014

Data set for Boston Map: https://data.boston.gov/dataset/city-of-boston-boundary

Data set from US Census: https://docs.digital.mass.gov/dataset/massgis-data-datalayers-2010-us-census

Data set for Boston Street: https://data.boston.gov/dataset/boston-segments/resource/a159f77f-3b3d-423d-a36f-6c804475817a

Data set for Boston Street 2: https://data.boston.gov/dataset/live-street-address-management-sam-addresses (Young prefers this)

Data set for Boston Main Street: https://data.boston.gov/dataset/main-street-districts (It does not have much, only 20)

## Optimize Polling Location
Optimize polling locations based on the accessibility to bus stops and MBTA
Use k-mean algorithms (SciKit) - Young
### Data Sets
Need four data sets: (All can be found at Analyze Boston)
1.	Bus Stops (Name, Coordinates) - Ray
2.	MBTA T station (Name, Coordinates) - Dennis
3.	Polling Location - Young
4.	Boston Map (jpeg file) / Boston Map Coordinates - Joyce
### How to transform data sets
These data sets are transformed to two new data sets
1.	optPollingByBus.py - Young
2.	optPollingByMBTA.py - ? (Follow optPollingByBus.py)
## Optimize shuttle bus stops
Optimize shuttle bus stops for the areas which are not accessible with bus stops and MBTA
First we need to divide Boston Map into 22 wards. I already put links of each data set for coordinates of Wards and Precinct. Please use geojson file from link and follow the steps explained during class.
### Data Sets
1.	Export the coordinates of Ward - ?
2.	Export the coordinates of Boston - Joyce
3.	Count the number of bus stops and MBTA station in each Ward - ? (constraint satisfatory problem)
4.	Find the coordinates of boston street - ?
### How to transform data sets
These data sets are transformed to a new data set
1.	mainStreet.py (Constraint Satisfactory Problem) - ?
2.	optShuttleBus.py - ?
