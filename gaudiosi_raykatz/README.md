# Data Mechanics Project 1
Project by Ben Gaudiosi and Ray Katz.

We retrieved datasets from the city of Boston API, the U.S. census, and the MBTA.

Several transformations were done in all three percentage files, the mbta_stop.py file, and zipcode_info.py file.

We weren't really sure how to use the dml library for authentication, so our scripts just load the files as dictionaries and read from them.

# Write up

Our project is to study and predict gentrification in Boston by utilizing and exploring much of the Boston publications on the web. The datasets we collected are as follows: coordinates for borders of Boston’s zipcodes, crime statistics, price per square foot of housing in Boston, various statistics about housing occupancy and age of structures from the U.S. census, racial demographics of Boston from the U.S. census, income statistics from the U.S. census, and finally, the routes and stops of the MBTA. These statistics can inform us in a variety of ways. First, we can use the map of Boston to later visualize our data. Crime statistics, price per square foot, and age of structures can show us where Boston could use the most improvement, but also where people may be most vulnerable. Racial demographics, income statistics, and housing statistics also show us where people may be most attracted to move to or where people would be most vulnerable. Finally, MBTA stops show us which areas are most accessible, and thus most attractive to new renters/homeowners. Overall, we find this to be a very interesting topic which Boston’s many open APIs can help us to learn more about.


# API Keys

The only needed API keys are from the U.S. census(http://api.census.gov/data/key_signup.html) and the MBTA (http://realtime.mbta.com/portal)
These are saved in the auth.json file as a json with the keys: "census" and "mbta"

# Dependencies

shapely

Install using

pip install shapely
