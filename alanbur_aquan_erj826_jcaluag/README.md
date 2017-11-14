Members:

github: bu_username
alanbur: osirissc2
aquan6: aquan
erj826: erj826
jcaluag: jcaluag

For project 2, we decided to shift our focus away from Boston and take a look at other large cities in the United States. We decided to look at New York City and San Francisco with hopes of studying and solving issues surrounding traffic accidents throughout different times of day. Our first tool is an implementation of K-means with an added optimization algorithm. We perform this algorithm on the New York City data and find the number of means that are necessary to cluster each accident to within a certain distance threshold of each mean. This metric could be used as a method to place police stations around the city. The second thing that we wanted to study is whether or not each borough of New York City behaves as a miniature city in terms of percentage of accidents that occur between certain hours. Our hypothesis is that they would, and that bordering boroughs may be more closely correlated than the others that are not touching. We generated a covariance matrix with the data from each borough, and compared it with the data from San Francisco. Through this we were able to confirm both aspects of our hypothesis.   


Execution Instructions:

* All our resource files are public and do not require authentication. 
* In bash, within the directory of alanbur_aquan_erj826_jcaluag, run python3 execute.py alanbur_aquan_erj826_jcaluag 
* This will go into the alanbur_aquan_erj826_jcaluag folder and run all the execute and provenance functions. 
* In the same directory, you can now view ‘provenance.html’ to see how the different entities were derived via activities by the agents in our project.


Resources:

1. New York Accidents: https://data.cityofnewyork.us/resource/qiz3-axqb.json

2. San Francisco Accidents: https://data.sfgov.org/resource/vv57-2fgy.json


Algorithms:

###EXPLAIN THESE###

K-means with an optimization:

Correlation coefficient comparison:
![alt text](https://github.com/aquan6/course-2017-fal-proj/blob/master/alanbur_aquan_erj826_jcaluag/covTable.jpeg)
