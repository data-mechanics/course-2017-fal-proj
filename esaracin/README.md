Eli Saracino
esaracin@bu.edu
U55135975


For this project, I thought it would be interesting to explore Boston's criminal justice system. Specifically, I was interested
in looking into the location of Boston's police stations and how they relate to the crime rates in corresponding policing districts. 
This would give some insight into how effective Boston policing is, and where its most lacking. To this end, I've collected several
datasets pertaining to this idea. These data are described in slightly more detail below. 

As will be detailed below, a huge driving force behind these scripts is Python's Pandas library, which is the main tool used to 
both convert .csv files to .json files and to perform all of the data transformations throughout.


Data Extraction (Project #1):
	There are three main DML scripts that work to extract data-- one for each data portal used. The services used
	are specified in each script's name, those being the following:

	city_of_boston_extraction.py: Used to scrape data from the city of boston data site. This provided the Boston
	shootings dataset.

	boston_opendata_extraction.py: Used to scrape data from Boston's opendata site. This provided both the set of data
	on police districts as well as the dataset on police stations throughout Boston.

	boston_gov_extraction.py:  Used to scrape data from the boston.gov site. Provided sets about crime incidents as 
	well as data about guns around Boston.

	As an intermediary step, Python's pandas library was used to parse any .csv files read from the web into .json files
	usable by Mongodb.


Data Transformation (Project #1):
	There are three DML scripts that work to transform my collected data in meaningful ways. As mentioned above, these
	transformations are almost entirely handled through the use of the Pandas library, which allowed the data to be stored
	in useful, DataFrame, objects for easy manipulation. A brief description of the scripts and their transformations are 
	detailed below:
	
	build_shooting_set.py: This script takes our boston_shootings dataset and projects two separate columns of information:
	the unique crime_types detailed in the shooting set, and the police districts in which those crimes occur. 
	Then, traversing the previously mentioned dataset, it uses these two projections to populate a dataset that details 
	the number of each type of gun crime for a given district. This new datset, shootings_per_district, is then saved to our MongoDB repo.

	merge_police_sets.py: This script takes our police_districts and crime_incidents datasets and aggregates the types 
	of crime in the latter district by type, finding a sum for each specific type of crime. This new column, which is 
	essentially a breakdown of crime variety in each district, is then joined with the original, police_districts, dataset 
	to construct a new, more fleshed out, picture of each of Boston's policing districts		

	join_sets.py: Takes to of our already manipulated datsets, police_stats, and shootings_per_districts, and starts by joining them on the DISTRICT column, 
	to generate a projection into a new dataset. To accomplish this join, I first had to rename the district column from the original, shootings, dataset 
	so that it had a match in the corresponding dataset. After the join, I iterate through the joined set, each representing a specific policing district 
	in Boston, and sum the types of shooting for that district. These totals are divided by the total crime count for that district, 
	to provide a percentage, for each district, of the number of shootings out of the total number of crimes. 
	To complete this projection, I effectively append this new column to the dataset, create the final, district_info, dataset.


Early Problem Solving (Project #2):
	In thinking of useful ways to consider the data collected and transformed in Project #1, some questions come to mind. Namely, how could I apply optimization problems and statistical analysis
	to these datasets in ways that are both variable and still meaningful?

	kmeans_crime_incidents.py: The first answer that came to mind was to apply K-means. Not only does it have a tunable input (that being the number of clusters you're looking for), but it applies
	an optimization function on the squared distance from each point to its cluster center. Whats more, it has real world applications here: in clustering the crime incident
	dataset collected in the previous project by the latitude and longitude each incident took place, I could find "crime centers" where crime was much more likely to happen around Boston.
	Currently, the algorithm searches for 5 clusters, because this is the number obtained from applying the elbow method to the chart included in this directory: 5 is the number of 
	clusters after which the error stops markedly decreasing. The beauty, though, is that whoever uses the algorithm could tune this parameter as they see fit; potentially, the City of Boston
	can only afford some number of patrol cars out and circulating at a given time. Using these crime centers, they could optimize the location of their patrols in a way to perfectly fits however
	many cars they are able to expend. Note that roughly 6.29% of the crime incidents did NOT include a location for the specified incident, and these data were dropped before clustering.

        race_linear_analysis.py: To get a closer look at how the BPD is dealing with crime in Boston, I also aquired a new dataset that documents FIOs, which required an update to the 
	boston_gov_extraction.py script. These Field Interrogation and Observations (essentially, stop and frisks) conducted by the BPD will give some insight into how the BPD is choosing
	who they target for stop and frisk. In this script, I perform linear regression on an input dataset containing the racial consistency of Boston's policing districts, and consider 
	any potential correlations between the targets chosen by the BPD for stop and frisk and the racial composition of a given policing neighborhood. The output attribute of this regression
	is the number of FIOs for a given Policing District, though this could also be changed to consider the number of crimes in a given neighborhood as well. This has some interesting implications,
	such as whether or not the BPD is biased in who it targets for stop and frisks, and how they might improve their current system.


Web Visualizations (Project #3):
	As the final component to the project, I had to develop two web-based visualizations of the data and the results to the problems I had tried to tackle. The first of these steps was to provide
	a more intuitive way to visualize the scope of Boston's crime problem, and its derivative FIO problem, through interactive web maps. This was approached in build_heatmap.py, which, reading in the 
	crime incident report data and FIO data obtained in Project #1, create an interactive .html file (heatmap_crimes.html) that allow the user to see how both crimes in Boston have changed over time. 

	With this more comprehensive view of the problem, I thought it best to at least attempt to devise a solution. The answer I came up with was simple, but elegant: simply cluster Boston into 
	it's respective crime centers, and provide a tool that would let any would-be user analyze exactly where they need to devote resources in order to effectively combat crime in Boston. In this way,
	the BPD could approach the problem of Boston's crime with as little bias as possible. This interactive tool is built in cluster_map.py and written to an .html file that can displayed in any 
	standard browser.

	Both Python scripts require the use of Python's Folium package, and a stable connection to MongoDB, from which the datasets to construct the .html files (which are already written and located in
	this directory) are loaded.


Note that all files pertaining to this submission are included in this directory (/esaracin/). auth.json, along with all other top-level files and directories,
were are not needed or modified in any way by the DML algorithms contained within this directory.
