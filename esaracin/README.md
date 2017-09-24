Eli Saracino
esaracin@bu.edu
U55135975


For this project, I thought it would be interesting to explore Boston's criminal justice system. Specifically, I was interested
in looking into the location of Boston's police stations and how they relate to the crime rates in corresponding policing districts. 
This would give some insight into how effective Boston policing is, and where its most lacking. To this end, I've collected several
datasets pertaining to this idea. These data are described in slightly more detail below. 

Data Extraction:
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
