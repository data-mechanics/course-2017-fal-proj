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


Early Problem Solving (Project #2): 6.29%, note elbow method
	In thinking of useful ways to consider the data collected and transformed in Project #1, some questions come to mind.	


Note that all files pertaining to this submission are included in this directory (/esaracin/). auth.json, along with all other top-level files and directories,
were are not needed or modified in any way by the DML algorithms contained within this directory.
