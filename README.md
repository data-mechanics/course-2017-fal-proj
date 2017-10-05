1.) For our first data set combination, we utilized Boston rain data (via Weather Underground) and data showing daily use of the Boston Public Library. Focusing on weather, we looked at whether rain in different months would affect the popularity of the library. This data could help with predicting how many people may come to the library in a given month, show which months demonstrated peaks, and possibly explain why. This data could be used to allow for the library to predict times of high utilization and prepare to serve it appropriately.

2.) For our second combination, we used data sets for colleges/universities in Boston (name, address, zip, and various stats), and residential properties (address, zip, classification, value, etc.). We looked at properties that were close to these colleges, compiling a list of ones that were within the same zip code. This list could be used for a number of reasons, from realtors finding suitable places to offer to college students, 	to potential home buyers wanting to know if they are close to a school or multiple schools and to expect student activity.

3.) For the third combination, we decided to look at Hubway locations to see which were close to colleges/universities, and find which had the most Hubway stops within half a mile. Those who bike using the hubway system may find this valuable being able to see potential college destinations. The Hubway company could potentially build new stations in locations that are lacking. It could possibly even be used to metric how bikeable a certain area around a college is.

Transformations:

1.) We took a collection of daily library visitor counts and combined them to form monthly averages. Then extracted average cm of rain per month from the Weather Underground data set. We then combined the two to form tuples of the following format to insert into the database: (Month, Avg Rainfall(cm), Avg Users). Note: rainfall data was from 2016 and library data ranged from 2014-2016

2.) We did a select for college names, addresses, and zip codes from the college/universities data set. Next we selected addresses, zip codes, and region (ex. East Boston) from the property assessment data set. We then aggregated properties and colleges together based on their zip codes and added a count




