### Motivation
Boston is known accross the US as one of the biggest college towns in the country, but what effect does this have on the city itself? 
With over 150,000 students in the city of Boston alone, schools and their young populations are bound to impact the 
surrounding area, just as the surrounding area impacts each school. We chose to use datasets pertaining to 3 main factors (social, safety, and accessibility) to create a weighted ranking for each university based on it's surroundings. To do this we have used the data sets listed below, and performed transformations that make this data useful for analysis. We then aimed to to first see what weight would establish the highest ranking for any individual school (detailed below under "Ranking Optimizer"), and then to use k-means in order to establish where the optimal placement for new police stations would be in order to have the highest impact on the safety scores (detailed under "New Police Stations").


### Datasets
###### Boston Colleges and Universities - https://data.boston.gov/dataset/colleges-and-universities

###### Boston Crime Incident Reports - https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system

###### MassDOT Car Crash Data - http://www.massdot.state.ma.us/highway/Departments/TrafficandSafetyEngineering/CrashData.aspx
###### - cleaned: http://datamechanics.io/data/jdbrawn_slarbi/CarCrashData.json
 
###### MBTA Bus Station Location Data - http://realtime.mbta.com/portal
###### - cleaned: http://datamechanics.io/data/jdbrawn_slarbi/MBTA_Bus_Stops.geojson
 
###### Boston Food Establishment Lisences - https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq

###### Boston Entertainment Lisences - https://data.cityofboston.gov/Permitting/Entertainment-Licenses/qq8y-k3gp

###### Boston Police Stations from City of Boston data portal - https://data.cityofboston.gov/resource/pyxn-r3i2.json

### Transformations
**transformation1** - Creates a union of the zipcode and business names from the entertainment and food license datasets. Then it takes the aggregate and returns a count of the number of 'social businesses' with the zipcode corresponding to those businesses. Ultimately this could be assigned some sort of weight in regards to how social a college is given the number of entertainment and food vicinities in its area.

**safetyTransformation** - This transformation ultimately makes a data set that includes each college and how many crimes and crashes are located within a mile of each school, in the form {'Name': , 'Number of Crimes': , 'Number of Crashes': } using the colleges dataset, crash data, and the crime incident reports. To do this we take the crash and crime data, and for each set, loop through every crash or crime for every college, and determine using the gpxpy library if each incident is within a mile of the school. If so we append to a list the name of the school, and the number 1, representing the incident. Then we aggregate these with the sum function to find the total number of crimes or crashes within a mile of each school. Now we have two of these sets, one with the totals for crime, and one with the totals for crashes, and we take the product of the two, select if the schools are the same, then project to just include the school and the two totals. We believe this new data set can be used to perfom an analysis on the safety of the surrounding areas of the different universities in Boston.

**mbtaTransformation** - This transformation ulimately makes a data set that includes each college, the number of MBTA bus stops located within a mile, and the number of students for that college, in the form {'Name': , 'Number of MBTA stops': , 'Number of Students': } using the colleges and MBTA stops datasets. To do this we first make a list aggregating the sum of the number of stops within a mile of each school in the same way we did above for crimes and accidents. We then take a list of the schools and their student populations which we have from the college data set, and similarly perform a product, selection, and projection on the two lists to combine the two statistics with the school name in one data set. We believe this data set could be used to perfom an analysis on the accessibility of transit for each school, factoring in student population for how many stops might be needed, or the density of availability.

**policeAnalysis** - Using the Boston Police station location data and the college dataset, this transformation determines the number of police stations within a mile of each school through distance calculations and sum aggregation.


### Ranking Optimizer
We started off by finding the individual scores per category per university in our safetyScore.py, transitScore.py, and socialScore.py scripts by normalizing the number of relevant datapoints within a mile of each school (e.g. number of bus stops for the transit score). From there we used these scores to find an overall ranking for each university assuming that each category were to be weighted equally in our overallScore.py script. 

In rankingOptimizer.py we aim to find the optimal weight for each category that would maximize an individual school's own ranking. The use case of this idea is from the perspective of an admissions office wanting to present the maximum possible ranking for their own university. We set a lower bound on the category weight to be 20% and the upper bound at 50% to eliminate the possibility of any school setting their highest category score as 100% or lowest as 0%. In order to play around with this, just change the SCHOOL_NAME variable at the top of the file to the school whose ranking you would like to maximize.

We found that new weights can actually have profound impacts on a particular school's ranking. For example Boston University School of Public Health's rating when optimized increased from 16/57 to 9/57, just by changing category weights from 33/33/33 to 44/36/20. On the other hand we found that Boston College is so bad, there is no possible weighting to improve their rank of dead last.


### New Police Stations
In safetyCorrelation.py we found a significant and very high positive correlation between the proximity of police stations to a university and it's safety score (0.77 with a p-value of 0). Because of this high correlation, we decided to try and find optimal locations for new police stations in order to improve the safety scores of the lowest scoring universities in that category. In other words, if the city was going to invest in a couple police stations, we want to find the best places to improve university safety accross the city where it is needed most.

In newStations.py, using our police stations data and the locations of universities with bottom half safety scores (score < 0.5), we use the k-means algorithm to find optimal placement for new police stations in order to improve safety scores for the lowest scoring universities. You can change the number of means run in our algorithm (i.e. number of new police station locations) by changing the NUM_CLUSTERS variable at the top of the file. We found 3 to work well.

### Web Service and Visualization
Details of our web service and visualization can be found in our Project Report [here](ProjectReport.pdf)

### Required Python Libraries
###### gpxpy - https://pypi.python.org/pypi/gpxpy
###### dml -   https://pypi.python.org/pypi/dml
###### prov -  https://pypi.python.org/pypi/prov
###### tqdm - https://pypi.python.org/pypi/tqdm
###### scikit-learn (sklearn) - https://pypi.python.org/pypi/scikit-learn/0.19.1
###### scipy - https://pypi.python.org/pypi/scipy

### Note
No additional credentials or API tokens are needed
