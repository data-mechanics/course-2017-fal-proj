# Analyzing Risk of Gentrification in Boston

By Ben Gaudiosi, Ned Geeslin, Ray Katz

## Introduction

In urban areas, gentrification is one of the most significant problems affecting low income communities. New buildings are put up, rent increases, and all of a sudden, the residents can no longer afford the rent to live in the place they call home. This has been a recurring throughout the neighborhoods of Boston in the past few decades and continues happening to this day. Our project analyzes the risk of this phenomenon in many of Boston’s neighborhoods by investigating various demographic, income, public transit, and housing statistics and developing a scoring system that ranks each zip code accordingly. Hopefully, with this ‘early warning’ system we are developing, communities will have the opportunity to measure the risk of this happening, and a chance to prevent it.

We collected our primary data sets from a variety of reliable sources. We used the 2010 U.S. Census  to pull information about racial makeup, married households, unemployed people, people in the labor force, and people taking public transit. Furthermore, the 2015 American Community Survey provided useful data pertaining to occupied and vacant housing, structures built before 1939, renter occupied homes, median income, median rent, and people in poverty. WIth the City of Boston’s ArcGIS map data, and the MBTA (routes and stops) we were able to get spatial data to use for out map. Using these, we turned many of these statistics into percentages for each district, with the exception of median income, median rent, and MBTA stops. To find the location of each MBTA stop, we had to take the coordinates of where each MBTA route stopped, and match that to a zip code. Furthermore, we compiled each set statistic into one table in MongoDB which could be identified by its zip code. Finally, with all this we were then able to create our analysis of the risk of gentrification in Boston.

## Analysis Techniques 

Gentrification is a process that happens over time, so we looked for correlations between factors in each neighborhood and quantify these factors. We performed two analyses - the first was developing a scoring algorithm for gentrification risk, and second was finding how specific statistics correlated in each neighborhood. In order to develop a way to score each neighborhood, we referred to a method used to create a warning system for gentrification in Berkeley, as seen in [1]. This paper identified several positives and negative indicators gentrification. We included some of our in our scoring algorithm which we also thought were relevant. For example, median income, unemployment, and access to public transportation were all used as indicators of gentrification.

Using the statistics for each neighborhood we gathered earlier, we calculated the mean and standard deviation of each variable, and used that information to normalize our statistics. With these now normalized statistics, we summed their values, multiplying by negative one for negative indicators. We did not do any additional weighing on each variable, as we could not find any way to quantify how much each factor determined whether a neighborhood was gentrifying. This sum, done on each zip code, represents our score that respective neighborhood. The biggest flaw in this scoring system is of course the lack of weighing for each variable. When attempted, we followed Berkerlee’s ranking to scale each factor, but the resulting scores were negligible. However, until further research is done, we did not feel as though we had sufficient information to make a judgment here.

The second analysis we performed was to find multiple interrelated correlations. Specifically, we wanted to see how median income and median rent correlated with each other, and then each of them with percent taking public transportation, unemployment, home occupancy, percent of old home, percent married, and racial makeup. We did this by finding the correlation coefficient between two variables. If that value was near zero, the two variables are likely uncorrelated, and if that value is closer to negative one or one, then those two values are negatively or positively correlated, respectively.

## Results

Figure 1: A table of scores for each neighborhood

| Zipcode | Score           |
| ------- | -------------------- |
| 02110   | -14.146108925066075  |
| 02210   | -12.090844969379752  |
| 02132   | -10.400958692661938  |
| 02109   | -9.623041479069807   |
| 02199   | -9.623011226284417   |
| 02108   | -6.123622178385251   |
| 02113   | -3.8977823124454027  |
| 02116   | -3.859269679643327   |
| 02163   | -3.4688718961758953  |
| 02136   | -2.567168276932776   |
| 02111   | -2.0250181672324192  |
| 02129   | -1.9818617760810455  |
| 02114   | -1.5432085488819267  |
| 02131   | -0.20682763065774168 |
| 02118   | 0.3293921498407694   |
| 02130   | 2.1077355688807575   |
| 02127   | 2.3227624166295566   |
| 02135   | 2.5411136855667102   |
| 02126   | 4.314800141581246	|
| 02125   | 4.644592913027217	|
| 02215   | 4.729251781889973	|
| 02134   | 4.963802931344546	|
| 02122   | 5.801403417237771	|
| 02128   | 5.888221417811069	|
| 02115   | 6.002357483931471	|
| 02124   | 6.916832487931011	|
| 02120   | 8.95440785975162 	|
| 02119   | 9.023774328771601	|
| 02121   | 13.01714717470246	|

Figure 2: A heat map of Boston. Red means an area is more gentrified, while yellow means and area is less gentrified.
![alt text](map.png "Heat map of gentrification risk in Boston")


From the above results, we can see that zipcodes 02119 (Roxbury) , 02120 (Roxbury Crossing), and 02121 (Dorchester) are at the highest risk of gentrification, while 02110 (Boston Harbor), 02210 (Children’s Museum area), and 02132 (West Roxbury) are at the least risk according to our scoring method. 

Figure 3: A table of correlations we calculated

| Correlation                                 	| Correlation Coefficient |
| ----------------------------------------------- | ------------------------|
| Median income/median rent                   	| 0.46839512347485085 	|
| Median income/percent taking public transit 	| -0.6454624318082117 	|
| Median income/unemployed                    	| -0.6982417162909252 	|
| Median income/percent homes occupied        	| -0.37042916575139995	|
| Median income/percent homes built before 1939   | -0.1369701760459735 	|
| Median income/percent white                 	| 0.7316812909047178  	|
| Median income/percent black                 	| -0.519794059870776  	|
| Median income/percent hispanic              	| -0.5223562724199077 	|
| Median income/percent asian                 	| -0.2244097883005071 	|
| Median income/percent married               	| 0.42025015853534775 	|
| Median rent/percent taking public transit   	| -0.3691003721240825 	|
| Median rent/unemployed                      	| -0.41922610137538596	|
| Median rent/percent spending 50% income on rent | 0.029052532399016718	|
| Median rent/percent homes built before 1939 	| -0.38728512866326253	|
| Median rent/poverty rate                    	| -0.6360178987098067 	|
| Median rent/bus stops                       	| -0.4371816233590174 	|
| Median rent/subway stops                    	| 0.10017178581177948 	|
| Median rent/percent married                 	| 0.012707094739366659	|

A few obvious correlations exist - median income and unemployment or median rent and poverty rate, for example. A few variables are also noticeably not correlated, such as the median rent and percent of people spending greater than 50% of their income on rent, or the median rent and percentage of married households. Unfortunately, it’s hard to draw conclusions from this data beyond the raw numbers, as we can’t infer what these correlations (or lack there of) actually mean without more research.

## Conclusion

Our analysis shows that parts of south Boston appear to have the greatest risk of being gentrified. Community leaders should look for solutions in regards to rent control so residents are not displaced. Looking ahead, it’s clear that more research needs to be done on how significantly each of the factors we used affects a neighborhoods gentrification risk so as to render a more accurate scale. Regardless, we believe our analysis provides an imperfect but reasonable picture of how gentrification is occurring in Boston, and hope this research will enable people to act before permanent damage is done.

## References

[1] Chapple, Karen. Mapping Susceptibility to Gentrification: The Early Warning Toolkit. UC Berkeley Center for Community Innovation, 2009.
