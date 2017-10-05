# course-2017-fal-proj

## Best location to open a restaurant

### Narrative

Find the best location to open a restaurant in the city of Boston based on the location’s nearby restaurants, crime rate, parking spaces and neighborhood’s income. Parking spaces is an important factor for people to consider whether to go to a restaurant. Crime rate indicates the safety of the location.  neighborhood’s income indicates the possibility of people going to eat out in the neighborhood. Number of restaurants indicate intensity of competition. 

### dataset:

1. parking meters in Boston
https://data.boston.gov/dataset/parking-meters
2. neighborhood in Boston
https://data.boston.gov/dataset/boston-neighborhoods
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
Use yelp api:https://www.yelp.com/developers/documentation/v3/
Please place the auth.json file in your folder{
	"yelp": {
			"CLIENT_ID" : "xxxxxxxxxxxxxxxxxxxxxx",
			"CLIENT_SECRET" : "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	}

}



