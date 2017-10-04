### Motivation
Boston is known accross the US as one of the biggest college towns in the country, but what effect does this have on the city itself? 
With over 150,000 students in the city of Boston alone, schools and their young populations are bound to have some impact on the 
surrounding area. With schools of various sizes spread all over Boston however, we are also interested in how each school's surroundings 
may affect their student population. To put ourselves in a position to be able to answer this question, we have decided to look 
at three categories of interest that can be rated and compared school to school: safety, social opportunity, and ease of tranportation. 
To do this we have used the data sets listed below, and performed transformations that make this data useful for analysis.

### Datasets
**Boston Colleges and Universities** - https://data.boston.gov/dataset/colleges-and-universities

**Boston Crime Incident Reports** - https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system

**MassDOT Car Crash Data** - http://www.massdot.state.ma.us/highway/Departments/TrafficandSafetyEngineering/CrashData.aspx
 - cleaned: http://datamechanics.io/data/jdbrawn_slarbi/CarCrashData.json
 
**MBTA Bus Station Location Data** - http://realtime.mbta.com/portal
 - cleaned: http://datamechanics.io/data/jdbrawn_slarbi/MBTA_Bus_Stops.geojson
 
**Boston Food Establishment Lisences** - https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq

**Boston Entertainment Lisences** - https://data.cityofboston.gov/Permitting/Entertainment-Licenses/qq8y-k3gp

### Transformations
**transformation1** - Creates a union of the zipcode and business names from the entertainment and food license datasets. Then it takes the aggregate and returns a count of the number of 'social businesses' with the zipcode corresponding to those businesses. Ultimately this could be assigned some sort of weight in regards to how social a college is given the number of entertainment and food vicinities in its area.

**safetyTransformation** - This transformation ultimately makes a data set that includes each college and how many crimes and crashes are located within a mile of each school, in the form {'Name': , 'Number of Crimes': , 'Number of Crashes': } using the colleges dataset, crash data, and the crime incident reports. To do this we take the crash and crime data, and for each set, loop through every crash or crime for every college, and determine using the gpxpy library if each incident is within a mile of the school. If so we append to a list the name of the school, and the number 1, representing the incident. Then we aggregate these with the sum function to find the total number of crimes or crashes within a mile of each school. Now we have two of these sets, one with the totals for crime, and one with the totals for crashes, and we take the product of the two, select if the schools are the same, then project to just include the school and the two totals. We believe this new data set can be used to perfom an analysis on the safety of the surrounding areas of the different universities in Boston.

**mbtaTransformation** - This transformation ulimately makes a data set that includes each college, the number of MBTA bus stops located within a mile, and the number of students for that college, in the form {'Name': , 'Number of MBTA stops': , 'Number of Students': } using the colleges and MBTA stops datasets. To do this we first make a list aggregating the sum of the number of stops within a mile of each school in the same way we did above for crimes and accidents. We then take a list of the schools and their student populations which we have from the college data set, and similarly perform a product, selection, and projection on the two lists to combine the two statistics with the school name in one data set. We believe this data set could be used to perfom an analysis on the accessibility of transit for each school, factoring in student population for how many stops might be needed, or the density of availability.

### Required Python Libraries
###### gpxpy - https://pypi.python.org/pypi/gpxpy
###### dml -   https://pypi.python.org/pypi/dml
###### prov -  https://pypi.python.org/pypi/prov



