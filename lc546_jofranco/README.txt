Introduction

Team: Lin Chen (GitHub: LiryChen) and Jesus Franco (GitHub:jesusfranco1)
Question: What’s the best location for a restaurant in Boston.

Factors:
1. Traffic: The number of T stops (red line) within 0.5 mi. If there is T stop nearby, it is easier to access the restaurant and the traffic expected to be higher, which can bring more walk-in business and customers from further part of the area.
Data source: http://developer.mbta.com/Data/Red.json

2. Economics: The medium income in the neighborhood, which is sorted by zip code, ideally higher than $40,000 per year. Many local restaurants are mainly supported by people whom live in the neighborhood. If on average people are richer in the neighborhood, the chances they eat out will be higher, and the revenue per person will be higher since they are more willing to pay more for good food.
Data source: https://data.cityofboston.gov/resource/rba9-vd7t.json

3. Traffic: The number of Hubway Bike station within 0.5 mi. The Hubway station, the bike renting station, can increase the accessibility of the restaurant, bringing more customers.
Data source: https://secure.thehubway.com/data/stations.json

4. Permit: The number of restaurant permit within 1 mi. If there are many restaurants nearby, the traffic will be increased, which potentially could bring more business.
Data source: https://data.cityofboston.gov/resource/fdxy-gydq.json

5. Safety: The number of crime happened in the neighborhood from 2012-2015. If it is a safer neighborhood, more customers are willing to return, and the risk of restaurant being stolen is lower.
Data source: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx

6. Vendor: The number of vendors nearby. A higher number of vendors implies a good traffic and more business.
Data source: https://data.cityofboston.gov/resource/xgbq-327x.json

7. Property: The accurate address, street name, zip code etc. of all the property in Boston area by City of Boston.
Data source: https://data.cityofboston.gov/resource/g5b5-xrwi.json

Datasets are from MBTA, City of Boston, and the Subway.

-----------------
 
Project 1:

Algorithms for three non-trivial transformations

1. Selection: This algorithm filters out all the redline T stops that are not in Boston.
2. Intersection: This algorithm find the restaurants that are in a street that had crimes.
3. Aggregation: This algorithm find the mean of the income of the population in a neighborhood.

-----------------

Please notice: we put size 500 in the trial mode and it will affect the accuracy of the results.
Files: correlationcoefficient.py, kmeans.py

Project 2:

Non trivial solutions for the problem, including k-means and statistical analysis

Our problem is to find the best location for a restaurant, and the primaries of a good location including high traffic, convenient public transportation access, large number of restaurants nearby, the number of houses nearby, and the crime rate of the neighborhood. 

First of all, we use the k-means to find in the clusters in Boston property data that indicate a large number of housing properties nearby. We believe a large amount of housing property nearby shows that the neighborhood is a busy area, and there are enough people live nearby to support local restaurants. 

Next, we use the statistical techniques to find the correlation coefficient of the number of restaurants near Hubway stations and the number of restaurants near the crime in the neighborhood. We assume that the safer neighborhood and higher traffic a place is, the more restaurants in that area. Our concern is if there is a positive correlation between two numbers, it implies that we may need to trade off the safety for the high traffic. However, after implementing the algorithm, we got that the correlation is 0.142, the covariance is 33.8, and the p-value is 0.002 which is smaller than 0.05. The statistics results assure that there is no positive relationship between the number of restaurants near Hubway stations and the number of restaurants near the crime in the neighborhood. It means the customers don’t care about if a place is safe or not when choosing dining option, or/ and high traffic doesn’t lead to a higher crime rate. 

Our suggestions for picking a great location for a restaurant are 1. find where the clusters are, and 2. choose a higher traffic area regardless even if the crime rate in the neighborhood is high. 
