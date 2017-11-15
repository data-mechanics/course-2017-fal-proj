#course-2017-fal-proj
Authors: Yingfeng Chen, Peter Gunawan, Xavier Bohorquez, Daniel Qiu
Date: 11/14/17
Assignment: Project #2

*************************
Important Notes to Notice:
	- we are using Scipy for k-means: therefore the Scipy package must be installed on the system
	- just a note in case the crimesProperty.py takes 10-15 seconds to start running even in trial - it is parsing through
		thousands of entries. it will be quick after that initial parsing
	- calculateCorrelations takes about 1 second per entry, it'll need to run 100 entries in trial mode, so that takes about 1.5 mins
*************************		

After combining our two groups together, we decided to keep the direction of the "happy hour problem" from project #1. We scrapped a lot of our
old datasets but also created new transformations and grabbed datasets from both the teams as well as one from an outside team. The datasets that
we have retrieved and have transformed into are as follows:

	- a dataset of College locations
	- a dataset of properties around Boston
	- a dataset of crimes marked around Boston
	- a dataset of Restaurants around Boston
	- a transformation into a set of # of crimes matched to # of properties per street
	- a dataset of k-means and radii

As previously described in Project #1, the "Happy Hour" problem within Boston is the fact that it is banned. Boston, and the entire state of MA, is
not allowed to host happy hours due to a drinking and driving accident caused by a fellow Bostonian decades ago. In order to bring back happy hours,
we decided to take it upon ourselves to come up with a metric/constraint that maximizes "safety" so that we can host happy hours again.

We decided to narrow the scope of our targeted project problem and as a result, our chosen solution is to find an optimal set of k-means coords and 
radii within the Boston area to allow restaurants to host Happy Hours based on our definition of safety. We are using the properties around Boston
and the crimes around Boston in order to matchup their addresses and quantities to create a new dataset that will let us see the # of crimes and #
of properties within each unique street address. This is where we will apply our first of two non-trivial problem solving algorithms!
	
	1) We are hoping to compare the property densities against the number of crimes for each street and see whether there is a high correlation
	between low density to crimes and high density to crimes. Although not implemented -- we are possibly thinking of adding a third correlation
	to compare for project #3 in terms of # crimes to college densities per town in Boston. To ensure that these correlations were of actual significance,
	we checked for the p-values of them as well. These correlations will be relevant after the next algorithm is explained as they will be used as a constraint!

We retrieved a dataset of Restaurant locations in Boston and parsed through all of them for their (x,y) coordinates. We plan on creating a 
visualization for Project #3 that will allow the user to see whether the restaurant is within the radii that allows happy hours. This is then our 
second non-trivial algorithm:
	
	2) After parsing through all of the Restaurant coordinates, we apply a k-means algorithm with all the coordinate points. We decided the K amount
	of means to use by applying the "Elbow Method" for k-means. The results of this method can be seen in our 'kmeans_elbowmethod.png'. How this
	method works is that we run the k-means algorithm a multiple of times (in our case for k = 1 to 10) and for each time we store the total
	distance between the chosen mean and all the points within its cluster. After running it enough times, the graph will look exactly like it is
	in our .png -- like an arm. The "elbow" part of the arm is where the method defines as the optimal amount of k means that will give accurate
	results. In our case, this turned out to be 5. So we ran the k-means algorithm with k=5 and with that we got the 5 most optimal mean coordinates
	that encompassses all of Boston's restaurants. To take this one step further, we ran through all of the coordinate points that were within
	each mean cluster and calculated the distance of the radii for each of the mean clusters based on taking the maximum distance between a mean
	and a point within the mean's cluster. By doing so, we have set up a core part of our project #3 design, which will be to determine whether
	a given restaurant's coordinates lies within any of the given radii. 
	
	2a) Currently all 5 computed k-means are in use. But this is where our non-trivial transformation with the correlation comparisons from 1) come in.
	After comparing all the correlations -- we will use the most significant correlation (as of now, it is the correlation between crimes and # properties).
	Our whole premise for this project is to create a set of radii that will ensure the highest level of safety. So using just the 5-means itself
	won't ensure that safety. But since we see that the correlation between # of crimes per street is extremely high with the # of properties per street,
	we will be using this as a constraint to determine in Project #3 which of the 5 k-means we will be keeping based relatively on the # of properties
	within each of the 5 clusters.
	
 


