# Data Mechanics Project 1
Project by Ben Gaudiosi and Ray Katz.

We retrieved datasets from the city of Boston API, the U.S. census, and the MBTA.

Several transformations were done in all three percentage files, the mbta_stop.py file, and zipcode_info.py file.

We weren't really sure how to use the dml library for authentication, so our scripts just load the files as dictionaries and read from them.

## Write Up

Our project is to study and predict gentrification in Boston by utilizing and exploring much of the Boston publications on the web. The datasets we collected are as follows: coordinates for borders of Boston’s zipcodes, crime statistics, price per square foot of housing in Boston, various statistics about housing occupancy and age of structures from the U.S. census, racial demographics of Boston from the U.S. census, income statistics from the U.S. census, and finally, the routes and stops of the MBTA. These statistics can inform us in a variety of ways. First, we can use the map of Boston to later visualize our data. Crime statistics, price per square foot, and age of structures can show us where Boston could use the most improvement, but also where people may be most vulnerable. Racial demographics, income statistics, and housing statistics also show us where people may be most attracted to move to or where people would be most vulnerable. Finally, MBTA stops show us which areas are most accessible, and thus most attractive to new renters/homeowners. Overall, we find this to be a very interesting topic which Boston’s many open APIs can help us to learn more about.


## API Keys

The only needed API keys are from the U.S. census(http://api.census.gov/data/key_signup.html) and the MBTA (http://realtime.mbta.com/portal)
These are saved in the auth.json file as a json with the keys: "census" and "mbta"

## Dependencies

shapely

Install using

pip install shapely


# Data Mechanics Project 2
Project by Ben Gaudiosi, Ned Geeslin, and Ray Katz

The first computation we performed is creating a scoring system for gentrification for each zip code, specifically done in averages.py and gentrification_score.py.
The second computation attempts to find correlations between the variables we have. This is done in stat_cor.py.


## Write Up

We decided to stick with the original project we chose of studying gentrification. We found a paper that details some of the indicators of gentrification:
https://communityinnovation.berkeley.edu/reports/Gentrification-Report.pdf
We used this paper to model our computations and to create a scoring system for gentrification, along with some of our own inputs. We first standardized most of the data that we collected in our first project, stored in the zipcode_info DB. 
Then, we found the number of standard deviations each indicator was from the mean for each zipcode, and summed these differences (multiplying by -1 for a few negative indicators). 
Our second computation is used to find correlations between the variables that we have in order to determine which factors are more or less significant when it comes to gentrification. 
For the most part, our results aligned with our intuitions: areas like the North End and Finanical District had low suspecibility to gentrification, while areas like Allston and Dorchester were more suspectible. 
Thus, we've created a way to rank the risk of gentrification in the various neighborhoods of Boston using the factors available to us. 

# Correlation Analysis
Gentrification is a proccess that occurs over time. Naturally, we look to examine the relation between each of the independent data sets.
Over time we hope to see a correlation or anti-correlation between statistics, and from this we will be able to identify which areas are gentified and or currently undergoing the process of gentrification


"Median income/transit=-0.6454624318082117" Average income and transit are interesting because it essentiall shows what class of people are using public transportation. 
It makes sense to see that they are realtively anti-correlated because 

"Median income/median rent=0.4683951234748508" When examinig the income to mediam rent we can see the average amount of disposable income for the different neigborhoods
We found that they were relatively correlated because as the average income of an area goes up so does quality of housing and cost of housing

"Median income/percent homes occupied=-0.3704291657513998"  there seems to be a slight anti-correlation but overall this does not indicate much other than these may be unrelated when studying gentrification

"Median income/unemployed" : -0.6982417162909252" These are strongly anti correlated which makes sense for obvious reasons, as the average income increases the number of homeless decrease per region

"Median income/percent homes built before 1930=-0.13697017604597347" The result we expect to see no correlation here as boston is an old city and old buildings are valued just as much as new, and older areas cant really
be gentrified if they are already wealthy


"Median income/percent white= 0.7316812909047176" Shows that an increase in white population correlates to an area undergoing gentrification.

"Median income/percent black=-0.5197940598707759"Interesting how income and percent black/hispanic are almost exactly equally ant-correlated. Suggests that decrease in populations is an indicator of gentrification

"Median income/percent hispanic=-0.5223562724199077"Interesting how income and percent black/hispanic are almost exactly equally ant-correlated. Suggests that decrease in populations is an indicator of gentrification

"Median income/percent asian=-0.22440978830050706" Uncorrelated

"Median income/percent married" : 0.42025015853534775, Somewhat correlated, suggesting married cupples and average income are related, however our research shows increased married couples is a negative indicator of gentrification

"Median rent/percent taking public transit" : -0.3691003721240824, slightly anti correlated but because we are in a city the relation is inconclusive

"Median rent/unemployed" : -0.41922610137538585, somewhat anti correlated. Intuitively this makes sense, as rent rises, unemployment decreases. 

"Median rent/percent spending 50% income on rent" : 0.02905253239901666, no correlation whatsoever, seems to be inconclusive

"Median rent/percent homes built before 1939" : -0.38728512866326253, somewhat correlated which makes sense because it is more expensive to live in historic areas of boston.

"Median rent/poverty rate" : -0.6360178987098066, relativelt anti correlated which is intuitive based on our metrics, as the average cost rent of an area increases, the amount of poverty decreases

"Median rent/bus stops" : -0.4371816233590172, somewhat anti correlated, while many areas have many bus stops, those who live in areas of lower rent and cost of living often use public transportation. Accoring to the article, an increase in public transportation is an indicator of gentrification

"Median rent/subway stops" : 0.10017178581177945, No correlation, shows that there are subway stops for all costs of housing

"Median rent/percent married" : 0.012707094739366676, No correlation, show's that rent and couples are unrelated and do not indicate gentrification

Summary:
Overall, it is important to look at all factor that are resulting in somewhat of a correlation. 
We compiled this information and created a gentrification scoring system which factors in correlation between data sets as well as ranking the value of the indicator. (Whether it shows in favor of gentrification or no-gentrification)
Because Gentrification is a process, and happens over time, the correlation between data sets was crutial in determining whether an region has demonstrated aspects of gentrification. The patterns in time make it easy to flag regions succeptable to gentrification. 
Furthermore, we cannot look at each individual correlation individually, we must view this as a proccess over time, so each component cannot be scrutenized, rather we must look at all factors of gentrification and make a broad assessment.
