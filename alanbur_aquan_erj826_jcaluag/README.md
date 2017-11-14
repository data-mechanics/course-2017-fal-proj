Members:<br />
github: bu_username<br />
osirissc2: alanbur<br />
aquan6: aquan<br />
erj826: erj826<br />
jcaluag: jcaluag<br />


For project 2, we decided to shift our focus away from Boston and take a look at other large cities in the United States.
We decided to look at New York City and San Francisco with hopes of studying and solving issues surrounding traffic accidents throughout different times of day.

Our first tool is an implementation of K-means with an added constraint satisfaction algorithm.
We perform this algorithm on the New York City data and find the number of means that are necessary to cluster each accident to within a certain distance threshold of each mean(at the moment we have hardcoded a 3 mile average distance between centroids and all the nodes in their cluster).
Our findings could be a factor used in deciding optimal placement of police stations, hospitals, or other response facilities around the city.

In our second area of study, we found data on the number of accidents per hour for San Francisco and for each borough in New York.
Our trial mode was implemented using reservoir sampling -- that is to say our subset of the whole set is obtained with a uniform distribution. If trial mode is on,
then data collection is unchanged and the three algorithm files will sample each dataset down to 100 before use.
With this data, we generated a covariance matrix that shows us how accidents between two areas correlate.
We added San Francisco as a baseline, because San Francisco is a whole other city, and should be less correlated to a NY borough
than two NY boroughs are. As you can see in the graph below, green means that a given New York bourough is more correlated than SF is to the corresponding borough, and red means less correlated.
With this data, we can find the higher correlated cities, and use them to make predictions or mimic protocol in regards to vehicle accidents.
![alt text](https://github.com/aquan6/course-2017-fal-proj/blob/master/alanbur_aquan_erj826_jcaluag/covTable.jpeg)



Execution Instructions:

* All our resource files are public and do not require authentication. 
* In bash, within the directory of alanbur_aquan_erj826_jcaluag, run python3 execute.py alanbur_aquan_erj826_jcaluag 
* This will go into the alanbur_aquan_erj826_jcaluag folder and run all the execute and provenance functions. 
* In the same directory, you can now view ‘provenance.html’ to see how the different entities were derived via activities by the agents in our project.


Resources:

1. New York Accidents: https://data.cityofnewyork.us/resource/qiz3-axqb.json

2. San Francisco Accidents: https://data.sfgov.org/resource/vv57-2fgy.json


