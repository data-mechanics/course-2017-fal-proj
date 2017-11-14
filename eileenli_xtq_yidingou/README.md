## Objective:
 Since Boston is a quite densely populated are in US, the quality of life is very important to everyone. We are exploring the rating of living around the schools in Boston by calculating the safety rate, the comfort rate, and the convenience rate. Our objective is to use k-mean to find the area that needs a hospital the most. Our safety rate will include data from crime, crash and hospitals. Our comfort rate will include data from entertainment and restaurants. Our convenience rate will include data from crash, hubway, traffic signals and MBTA. 

# Databases:
1. Crash: http://datamechanics.io/data/eileenli_xtq_yidingou/crash.json
2. MTBA: http://datamechanics.io/data/cyung20_kwleung/mbta-t-stops.json
3. Hubway: http://datamechanics.io/data/eileenli_xtq_yidingou/Hubway_Stations.geojson
4. Schools: http://datamechanics.io/data/eileenli_xtq_yidingou/Colleges_and_Universities.geojson
5. Restaurants: http://datamechanics.io/data/eileenli_xtq_yidingou/Restaurant.json
6. Crime: http://datamechanics.io/data/eileenli_xtq_yidingou/crime.json
7. Hospitals: http://datamechanics.io/data/eileenli_xtq_yidingou/hospital.json
8. Entertainment: http://datamechanics.io/data/eileenli_xtq_yidingou/new.json
9. Traffic signals: http://datamechanics.io/data/eileenli_xtq_yidingou/Traffic_Signals.geojson


# Process:
1. Extracting data from databases:
a). Comfort Section: we extract the coordinates of every entertainment from Entertainment database and coordinates of every restaurant from Restaurant database, and put them into a new dictionary.
b). Safety Section: we extract the coordinates of every crime insident from Crime database, the coordinates of every car crash from Crash database, and the coordinates of hospitals from Hospital database, and put them into a new dictionary.
c). Convenience Section: we extract the coordinates of every car crash from Crash database, the coordinates of every hubway from Hubway database, the coordinates of every traffic signals from Signals database and the coordinates of every MBTA from MBTA database, and put them into a new dictionary.

2.	Data Relation to School:
We first extracts the coordinates of every school from school database, and to calculate the distance from every coordinate of entertainment, restaurant, crime, crash, hospitals, hubway, traffic signals and MBTA. Then we will find the coordinates of those places that are within 2 miles from each school and put them into a new disctionary called "schoolfinal" such as {"school": i["properties"]["Name"],"properties": [{"hospital": hospital},{"crime": crime},{"crash": crash},{"restaurant": restaurant},{"entertainment": entertainment},{"hubway": hubway},{"traffic signal": signal},{"MBTA": MBTA},{"safety": (2000 + hospital*2 - crime*2 - crash) / 100},{"comfort": (restaurant + entertainment) / 100},{"traffic": (1500 + MBTA + hubway - signal - crash * 2) / 100}]}. 

3.	Statistics Relation to School:




4. K-means Analysis for best hospital place:
We use the k means algorithm to find the optimal hospital place to show where needs the hospital the most base on the rates of comfort, safety and convenience. We have the score for the amount of hospitals around each school, with the k mean, we can form many clusters, and the optimal place for government to buid a hospital is the cluster with the lost score.



 