# course-2017-fal-proj

This repository pertains to CS591 - Fall 2017 taught by Andrei lapets.
**This project description will be updated as we continue work on the infrastructure.**

## Narrative:
```
    The purpose of our project was to gather data on overweight persons in the Boston city area, and calculate 
    whether or not there exists a correlation between income/property values and the number of overweight people
    in then vicinity. Furthermore, we gathered data on the location of current winter food markets and data on 
    failed health inspections and their corresponding locations. This allows us to map/plot the locations that
    have poor health standards and access to safe/healthy food. These two data points allow us to make a 
    constrained decision of the optimal location of health food stores. Our findings are constrained to just 
    the Zipcodes that are registered with the city of Boston (I.E Brookline is not included in our findings). 
    Our data points are further constrained to just landmass, as optimal locations could indicate a non-viable 
    placement for a store/restaurant.
```


## Data Sets:

1. #### getHealthInspections.py 
```
    Retrieves data about HealthInspections from data.boston.gov and deposits the data in an instance of MongoDB.
```
2. #### getObesityData.py 
```
    Retrieves 16000 data points regarding obesity statistics from the CDC website.
    This is then added to the instance of MongoDB for further manipulation downstream.
```
3. #### getOrganicPrices.py 
```
    Retrieves data of the average price for particular food items
    during the 12 months of the year. This data is not used downstream.
```
4. #### getPropertyValues.py 
```
    Retrieves data on different types of buildings/homes in boston as well
    as other descriptors of value from data.boston.gov. This data is used
    downstream for a correlation between obesitry and income.
```
5. #### getWinterMarkets.py
```
    Retrieves data on the location of seasonal markets in the boston area from data.cityofboston.gov.
    This data is deposited into the MongoDB instance and is used downstream to calculate the distance 
    of optimal placements of markets based on obesity statistics.
```
6. #### getZipCodeData.py
```
    Retrieves a mapping object of the zipcodes in Boston to their respective districts 
    and town names. This is used downstream as a metric of joining some datapoint 
    fields based on ZipCodes.
```
7. #### setHealthPropertyZip
```
    Retrieves the data generated by getHealthInspections.py, getPropertyValues.py, 
    and getZipCodeData.py and runs multiple aggregations, projections, selections, 
    and transformations on the data to properly format the data to place back in mongoDB.
```
8. #### setObesityMarkets.py 
```
    Retrieves the data generated by getObesityData.py and getWinterMarkets
    and runs K means to determine the optimal location for Winter Health Markets.
```
9. #### getBostonZoning.py
```
    Retrieves a GeoJson of the landmass of the Greater Boston and area and deposits
    it in MongoDb.
```

10. #### setObesityPropertyCorrelation.py
```
    Reads the datasets on Obesity and Property Values and first maps each overweight 
    person to the properties around them. The amount of overweight individuals to each 
    property is then condensed into ranges (incremented by 100,000). This bucked data is 
    then used to calculate the correlation coefficient of obesity to property values and 
    the output is loaded into MongoDB. Our findings showed the expected results, that there 
    are more overweight people in areas with lower property values.
```

## Instructions:
```
    -  In order to run the code:
        - run mongodb via the "mongod" command (May need SuperUser Permissions)
        - run "mongo reset.js" and "mongo setup.js"
        - run "python3 execute.py biel_otis" to execute the code
        - The code may take up to 10 minutes to run and produce a provenance diagram
```

## Requirements:
```
    - Multiple Python libraries are required for this project:
        - geopy (pip install geopy)
        - sklearn (pip install sklearn)
        - scipy (pip install scipy)
        - dml (pip install dml)
        - prov (pip install prov)
```

#### James Otis and Max Biel (2017)
