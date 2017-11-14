# Narrative

For this project, we have decided to answer a certain question that is essential to solve in city enviornments. What factors contribute to criminal activities? To answer this question, we have taken certain data that we believe may have an impact on the crimes that occur in the Boston Area. We transformed datasets involving crime reports, MBTA schedules, property values, employee earnings, and education info in order to further analyze the impact each of these factors may have had on criminal activity in the Boston Area.

# Datasets

## Analyze Boston
getCrimes.py - retrieves crime report data
getProperties.py - retireves property earnings data from various streets in Boston

## GitHub MassBigData/LateNightT
getMBTA.py - retrieves MBTA data

## College Scorecard Data
getCollege.py - retrieves data on Colleges around boston

## City of Boston
getEarnings.py - retrieves data on employee earnings around boston

Note: datasets from GitHub MassBigData/LateNightT, College ScoreCard Data, and City of Boston were formatted and cleaned and then uploaded onto datamechanics.io

## Transformations

crimesSorted.py - creates a collection that groups all crimes that occur based on their offense group code, and counts the amount of time each offense has occured

crimesProperty.py - creates a collection that takes a crime and the street name the crime occured on, and the property value data that corresponds to that street.

mbta_ln.py - creates a collection that groups late night routes based on weekday.
