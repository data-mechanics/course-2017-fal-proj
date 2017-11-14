Members:

github:bu username

alanbur:

aquan6: aquan

erj826: erj826

jcaluag: jcaluag

For project 2, we decided to shift our focus away from Boston and take a look at other large cities in the United States. We decided to look at New York City and San Francisco with hopes of studying and solving issues surrounding traffic accidents throughout different times of day. Our first tool is an implementation of K-means with an added optimization algorithm. We perform this algorithm on the New York City data and find the number of means that are necessary to cluster each accident to within a certain distance threshold of each mean. This metric could be used to minimize the number of police stations around the city (by placing police stations at each mean). The second thing that we wanted to study is whether or not each borough of New York City behaves as a miniature city in terms of percentage of accidents that occur between certain hours. Our hypothesis is that they would, and that bordering boroughs may be more closely correlated than the others that are not touching. We generated a covariance matrix with the data from each borough, and compared it with the data from San Francisco. Through this we were able to confirm both aspects of our hypothesis.   


