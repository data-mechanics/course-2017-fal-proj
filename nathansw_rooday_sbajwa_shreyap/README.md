# CS591 Project 1

Many residents in the City of Boston rely on the MBTA for transportation to and from work. Hence, the reliability of public transportation is of utmost importance to the city residents. Moreover, Boston neighborhoods are a diverse landscape; each varying in its culture and demographics.

What we attempt to analyze as part of this project is two-fold:

First, we focus on exactly "how" these neighborhoods contribute to the reliability of public transportation in Boston. Regression analysis on the reliability of public transport routes alongwith the neighborhoods they pass through, gives us a way to ***quantify*** how much each neighborhood contributes to the overall reliability metric. 

__How is this helpful to policy makers?__

Our algorithm is fully paramterizable, and can predict reliability of a route you supply to it. If policy makers wish to implement a new T route, our algorithm can predict the reliability of the new route based on which neighborhoods it passes through, allowing descision makers to compare it among existing reliability statistics and making informed choices about implementing that route.


Second, we dive into the wealth of demographic data which is available for Boston's neighborhoods,such as poverty statistics, median household income, race, and means of commuting etc. We attempt to see how tightly **geographical proximity** among neighborhoods is linked to (or can influence) the spread of this demographic across neighborhoods. For example, one way to think about this can be:

> Given a neighborhood A, how does geographical proximity influence the spread or trickling of poverty from neighborhood A to its surrounding neighborhoods?

We do this by analyzing a distance matric between neighborhoods and computing its correlation with various demographic data.
The coorelation provides us a sizeable measure of this "trickling"

__How is this helpful to policy makers?__

When we do the above analysis for *all* neighborhoods, we get a influencing score for each of them. This can be visualized as a heatmap, such as below:

![Alt text](trickling_effect.png?raw=true "trickling Effect Heatmap")

The more red a box, more is its tendency to influence the trickling of that demographic into its adjacent neighborhoods.

Policy makers can make use of this heatmap for a particular demographic to answer some questions about implementing new policies. For example, looking at above heatmap for the Poverty section, we see that Charlestown has most tendency to influence poverty in its surrounding areas. So while implementing an reformative implementation for this neighboorhood, we can assure that the change will trickle to its neighbors as well. But for something like Allston, we would have to implement the policy for Allston as well as its adjacent neighborhoods (because there is no influence there)


## Notes on MBTA Performance Data:

Data regarding the reliability of the MBTA was collected from [MBTA Performance Dashboard](http://www.mbtabackontrack.com/performance/index.html#/download). This site provides historical data regarding the on time performance of the MBTA's three major types of mass transit vehicles: the bus, the rail, and the commuter rail. The following is a description of the column names provided in the original csv that were also included in our modified JSON file: 
* *SERVICE_DATE* - Date at which the data was collected
* *PEAK_OFFPEAK_IND* - Indicates whether the data was collected during on or off peak periods
* *MODE_TYPE* - Chosen from the options of bus, rail, or commuter rail 
* *ROUTE_OR_LINE* - Gives the bus route number (ex: 57) or rail line (ex: Green Line or Rockport Line)
* *METRIC_TYPE* - Dependent on the mode type. Each mode of transportation collects a different metric to calculate their on time performance. Therefore, **do not aggregate data across mode types** . The metric given here will indicate how to interpret the OTP numerator and denominator
  * Headway/Schedule Adherence - For the bus and commuter rail
  * Passenger Wait Time - For the rail 
 * *OTP_NUMERATOR* - OTP = On Time Performance
    * Bus - Number of key stops served on time
    * Commuter Rail - Number of trains reaching their final stop less than 5 minutes late
    * Rail - Number of passengers who waited longer than scheduled time between trains
 * *OTP_DENOMINATOR* 
    * Bus - Total number of key stops (number of trips run on a route in a day x number of key stops on that route)
    * Commuter Rail - Total number of scheduled trains
    * Rail - Estimated number of passengers waiting at the station
  
OTP numerator and denominator values are given (rather than %) to make it easier to aggregate within a mode. 
The original csv file was converted to a JSON file that follows the format of a nested dictionary: 

{SERVICE_DATE: 
  
    {1: {PEAK_OFFPEAK_IND: x1, MODE_TYPE: y1, ROUTE_OR_LINE...}, 
  
    {2: {PEAK_OFFPEAK_IND: x2, MODE_TYPE: y2, ROUTE_OR_LINE ...}, 
 
 ...}
 
The keys to the JSON object are the dates in which performance data was collected. The values to each key are dictionaries representing each record of data collected for the given date. The data was organized this way because there were multiple entries for each date.
