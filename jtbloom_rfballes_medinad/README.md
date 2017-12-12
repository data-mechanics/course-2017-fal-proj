# CS 591 Fall 2017 Final Project
### Boston Hubway Trips in Relation to Average Neighborhood Income and Population 
Jake Bloomfeld (jtbloom@bu.edu), Ricardo Ballesteros (rfballes@bu.edu), Daniel Medina (medinad@bu.edu)

#### Introduction
Hubway is a public bike-share system serving the people of Boston, Brookline, Cambridge, and Somerville. With roughly 1800 bikes in almost 200 stations, the Hubway serves as a fun, affordable, and convenient transportation option for quick trips around Boston and its surrounding municipalities. In 2016 alone, over 100,000 people took roughly 1.2 million trips totaling just over 2.8 million miles. As sustainable transportation methods are becoming more popular in cities, we had questions of whether or not a relationship exists with the frequency of trips within neighborhoods and that neighborhood's average income. With some further thought, we proposed our final questions: is there a correlation between income per capita in Boston's neighborhoods with the number of Hubway trips taken in that neighborhood? Also, is there a correlation between neighborhood population with Hubway trips taken in that neighborhood? Seeing if such relationships exists can pose further, more interesting points for discussion and analysis. We would be able to examine whether more trips are being taken to and from wealthier neighborhoods than poorer neighborhoods. This could be useful in seeing if the socioeconomic conditions of a nerighborhood have any effect on the frequency of trips to and from that neighborhood. Also, we would be able to examine whether neighborhoods with a higher population density have a high or low frequency of trips, potentially raising the concern of over/underutilizing bikes in that specific neighborhood. These are the questions that we plan to tackle in our project.

#### Data Sets
To solve this problem and perform the necessary analysis, we use and manipulate several datasets as described below:
<br><br>
*Hubway Station Locations* <br>
* Description: This dataset consists of the name of each Hubway station, its longitude and latitude coordinates, number of bike docks, and the municipality it resides in.
* Source: Analyze Boston.
<br><br>
*Hubway Trip History* <br>
* Description: This dataset consists of trip histories of a given month, including the trip duration (in seconds), start time and date, stop time and date, start station name & ID, end station name & ID, bike ID, user type (Casual = 24-Hour or 72-Hour Pass user; Member = Annual or Monthly Member), and zip code (if member). 
* Source: Hubway System Data.
<br><br>
*Per Capita Income by Boston Neighborhood* <br>
* Description: This dataset consists of each Boston neighborhood with its average income per capita. 
* Source: Census Bureau
<br><br>
*Population by Boston Neighborhood* <br>
* Description: This dataset consists of populations by Boston neighborhoods.
* Source: Census Bureau
<br><br>
*Boston Neighborhoods* <br>
* Description: This dataset GeoJSON polygons, which are enclosed areas within certain geographical coordinates that represents the different neighborhoods in Boston.
* Source: Boston OpenDataSoft
<br><br>

#### Methods
Once we gathered the datasets, we had to perform some transformations on them in order to get them into the state that would be easiest for us to work with. Due to the fact that we only had average per capita income and population by Boston neighborhood, we had to filter out all of the Hubway stations in the Hubway Station Locations dataset that were within the Boston municipality (i.e.Brookline, Cambridge, Somerville, etc.). For the Hubway Trip History dataset, we filtered out trip durations, start/stop times and dates, bike IDs, user type and zip code. Our main use for this dataset was to calculate the frequency of trips in each station, so we didn't have any need for extraneous information. For the remaining three datasets, we did not have to manipulate much as the original data was already in good shape to use.
<br><br>
The first step we had to take was to figure out which Hubway stations fall in which neighborhood in Boston. In the Hubway Station Locations dataset, we were given station names and geographical coordinates, so we had to map the coordinates to a specific Boston neighborhood. Given the coordinate points of the station, we checked if the station is in certain Boston neighborhood, given by the Boston Neighborhood Geoshape dataset, and added a neighborhood attribute to each Hubway statio. Once all Hubway stations were mapped to a certain Boston neighborhood, we were then able to use the Average Per Capita Income and Average Population by Neighborhood datasets.

#### Files
* stationsByneighborhood.py: This script is used to find out which Hubway stations fall in which Boston neighborhood. 
* neighborhood_station_income_out.py: This script is used to aggregate the number of trips that come out of each neighbprhood in order to execute the statistical analyses.
* trips_income_correlation.py: This script executes a correation between the average per-capita neighbprhood income and Hubway trips within that neighborhood.
* trips_population_correlation.py: This script executes a correation between the neighbprhood population and Hubway trips.
* neighborhood_income.py: This script is used to transform the data from out income per capita by neighborhood data set. It's used to aggregate trips and to again execute a correlation. <br><br>

##### Note
Be sure to run get_datasets.py before anything else.
In order to run the two correlation files newbosneighborhoods.py, outgoing_trips.py, neighborhood_income.py, neighborhood_station_income_out.py must be run beforehand.

#### Results
In order to perform statistical analysis, we used the Pearson product-moment correlation coefficient to determine the strength and direction of the linear relationship between the number of Hubway trips per neighborhood with its average income and population. Using a sample of trips taken in one month, we were able to obtain the following results: <br><br>

Correlation between # of Hubway Trips and Average Per-Capita Income by Neighborhood: <br>
Correlation Coefficient: -0.376  
P-value: 0.133
<br>
<br>
Correlation between # of Hubway Trips and Average Population by Neighborhood: <br>
Correlation Coefficient: 0.377
P-value: 1
<br>
<br>

From our results, we were able to generate two scatterplots to visualize these relationships:

![alt_text](https://github.com/rfballesteros/course-2017-fal-proj/blob/master/jtbloom_rfballes_medinad/trips-income%20correlation.png "Correlation between Hubway Trips and Average Per-Capita Income by Neighborhood")
 
![alt text](https://github.com/rfballesteros/course-2017-fal-proj/blob/master/jtbloom_rfballes_medinad/trips-population%20correlation.png "Correlation between Hubway Trips and Average Neighborhood Population")


#### Conclusion
Based on our calculations there is a moderate negative correlation between the number of Hubway trips per neighborhood and average neighborhood income. There is also a moderate positive correlation with the average neighborhood population with a high p-value, meaning that this observation is a non-significant result. Based on these results, we can learn that neighborhoods with higher frequencies of trips tend to have a lower average per-capita income. 

#### Visualization
We created a visualization to display our findings using Leaflet, a JavaScript library for interactive maps. This interactive visualization includes four buttons: Income per Capita, Population, Hubway Trips, and Neighborhoods. The Income per Capita feature displays red circles in each neighborhood, with the radius of the circle proportional to the neighborhood's average per capita income. The Population feature displays blue circles in each neighborhood, with the radius of the circle proportional to the neighborhood's population. Next, we added the Hubway Trips feature, which displays lines between the start and end Hubway station locations for all of the Hubway trips we used in our analysis. Note that this takes some time to load, as it loads over 100,000 trips. The final button toggles the neighborhood borders to give a better sense of the area of that neighborhood. See below for screenshots of our visualization:

![alt_text](https://github.com/rfballesteros/course-2017-fal-proj/blob/master/jtbloom_rfballes_medinad/visualization1.PNG)

![alt_text](https://github.com/rfballesteros/course-2017-fal-proj/blob/master/jtbloom_rfballes_medinad/visualization2.PNG)

![alt_text](https://github.com/rfballesteros/course-2017-fal-proj/blob/master/jtbloom_rfballes_medinad/visualization3.png)


#### Future Work
For expanding on this project in the future, there are several aspects to consider:
* Combining Hubway trip history data sets from past several years to get more accurate correlation data
* Using datasets of income per capita and population in municipalities outside of Boston (Brookline, Cambridge, Somerville) to get a bigger picture.
