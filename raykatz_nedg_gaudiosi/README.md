# Data Mechanics Project 1
Project by Ben Gaudiosi and Ray Katz.

We retrieved datasets from the city of Boston API, the U.S. census, and the MBTA.

Several transformations were done in all three percentage files, the mbta_stop.py file, and zipcode_info.py file.

We weren't really sure how to use the dml library for authentication, so our scripts just load the files as dictionaries and read from them.

## Write Up

Our project is to study and predict gentrification in Boston by utilizing and exploring much of the Boston publications on the web. The datasets we collected are as follows: coordinates for borders of Boston’s zipcodes, crime statistics, price per square foot of housing in Boston, various statistics about housing occupancy and age of structures from the U.S. census, racial demographics of Boston from the U.S. census, income statistics from the U.S. census, and finally, the routes and stops of the MBTA. These statistics can inform us in a variety of ways. First, we can use the map of Boston to later visualize our data. Crime statistics, price per square foot, and age of structures can show us where Boston could use the most improvement, but also where people may be most vulnerable. Racial demographics, income statistics, and housing statistics also show us where people may be most attracted to move to or where people would be most vulnerable. Finally, MBTA stops show us which areas are most accessible, and thus most attractive to new renters/homeowners. Overall, we find this to be a very interesting topic which Boston’s many open APIs can help us to learn more about.


## API Keys

The only needed API keys are from the U.S. census(http://api.census.gov/data/key_signup.html) and the MBTA (http://realtime.mbta.com/portal)
These are saved in the auth.json file as a json with the keys: "census" and "mbta"

## Dependencies

shapely

Install using

pip install shapely


# Data Mechanics Project 2
Project by Ben Gaudiosi, Ned Geeslin, and Ray Katz

The first computation we performed is creating a scoring system for gentrification for each zip code, specifically done in averages.py and gentrification_score.py.
The second computation attempts to find correlations between the variables we have. This is done in stat_cor.py.


## Write Up

We decided to stick with the original project we chose of studying gentrification. We found a paper that details some of the indicators of gentrification:
https://communityinnovation.berkeley.edu/reports/Gentrification-Report.pdf
We used this paper to model our computations and to create a scoring system for gentrification, along with some of our own inputs. We first standardized most of the data that we collected in our first project, stored in the zipcode_info DB. Then, we found the number of standard deviations each indicator was from the mean for each zipcode, and summed these differences (multiplying by -1 for a few negative indicators). Our second computation is used to find correlations between the variables that we have in order to determine which factors are more or less significant when it comes to gentrification. For the most part, our results aligned with our intuitions: areas like the North End and Finanical District had low suspecibility to gentrification, while areas like Allston and Dorchester were more suspectible. Thus, we've created a way to rank the risk of gentrification in the various neighborhoods of Boston using the factors available to us. 
