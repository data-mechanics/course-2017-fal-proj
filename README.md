# course-2017-fal-proj
Joint repository for the collection of student course projects in the Fall 2017 iteration of the Data Mechanics course at Boston University.

In this project, you will implement platform components that can obtain a some data sets from web services of your choice, and platform components that combine these data sets into at least two additional derived data sets. These components will interct with the backend repository by inserting and retrieving data sets as necessary. They will also satisfy a standard interface by supporting specified capabilities (such as generation of dependency information and provenance records).

**This project description will be updated as we continue work on the infrastructure.**

## Best location to open a restaurant

### Narrative

Find the best location to open a restaurant in the city of Boston based on the location’s nearby restaurants, crime rate, parking spaces and neighborhood’s income. Parking spaces is an important factor for people to consider whether to go to a restaurant. Crime rate indicates the safety of the location.  neighborhood’s income indicates the possibility of people going to eat out in the neighborhood. Number of restaurants indicate intensity of competition. 

### dataset:

1. parking meters in Boston
https://data.boston.gov/dataset/parking-meters
2. neighborhood in Boston
3. income in Boston
http://datamechanics.io/data/wenjun/censusincomedata.json
4. yelp dataset
https://www.yelp.com/developers/documentation/v3/business_search
5. crime rate in boston
https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system

### transformation:

1. find 100 means in parking meters
2. number of crime within certain distance of location of that 100 means
3. number of restaurants within certain distance of location of that 100 means

### auth.json
Please place the auth.json file in your folder
Use yelp api:https://www.yelp.com/developers/documentation/v3/
{
	"yelp": {
			"CLIENT_ID" : "xxxxxxxxxxxxxxxxxxxxxx",
			"CLIENT_SECRET" : "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	}

}



