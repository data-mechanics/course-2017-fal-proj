# CS591 Project 1

Many residents in the City of Boston rely on the MBTA for transportation to and from work. It is also no secret that Boston's diverse weather throughout the year can take a toll on MBTA service - one notable example from recent memory would be the Winter of 2014-2015. The question we want to ask is how can MassDOT, the MBTA, and the governments of Boston and Massachusetts direct their resources in such a manner that communities that depend moreso on public transportation than others get the attention they need? Factors such as incliment weather would be explored with relevant datasets, as well as demographic data on various neighborhoods around Boston including but not limited to median household income, race, and means of commuting.

## MBTA Performance Data

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

## Analysis 2

#TODO clean this up and add heatmaps from analysis with explanation.

* For each neighborhood, 
  * get all neighborhoods within a N mile distance
	 * Make a DF with each neighborhood pair as rows, columns being geographical distance between neighborhoods and a demographic dimension such as 		difference in crime incidents between those neighborhoods
	 * Compute correlation between the two columns of DF
		* A high positive correlation implies that as geographical distance decreases, the difference in demographic dimensions also decreases, which implies 			that neighborhood A and those around it are similar in crime incidents, and hence serving as a crime hotspot. We can also assume that this hotspot is 			centered at A since we compute the correlation wrt A
		
  * We store the correlation value alongside that neighborhood
  * Once we have a correlation value with each neighborhood, we can build a heat map of neighborhoods, the gradient being how tightly correlated that 			neighborhood is with those around it in terms of different statistics- crime/income/poverty
	
 *	How can this help policy makers?
	
	Lets take the case for crime. Using the heat map, policy makers can immediately identify key crime areas that have a tight correlation with nearby neighborhoods, and those serve as target/priority focal points for them to implement any reformation policies. The policy can be applied to the highly correlated bunch of neighborhoods in order of priority.

This could be done for many other reformative measures, such as poverty alleviation, 311 Violations, etc

