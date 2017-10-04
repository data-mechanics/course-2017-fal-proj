# The Purpose of Project

As an every presidential election, every election is the most important election in our lifetime before knowing the result of election, because we can change the result by voting. However, based on the research done by our group, more than 40% of U.S citizen did not vote in 2016. With researching the relationship between voter turnout and the other factors, we find the journal: "Increasing Voter Turnout: Can Mass Transit Help?" (http://www.inquiriesjournal.com/articles/1618/increasing-voter-turnout-can-mass-transit-help). By using dataset from various data sources of Boston, we focus on finding the relationship between voter turnout and public transportation.

## Authors
Young Jun Choi (yjunchoi)
Yuchen Zhang (yzhang71)
## Data Retrieval

We collected 6 data sets from MBTA, Analyze Boston, and US Census.
1. averageDelay.py
  For this data set, we retrieve the average delay time between MBTA stations in Boston area from MBTA GitHub.
2. Station_Links.py
  For this data set, we collect the data of each line and its color from MBTA GitHub.
3. Station_Node.py
  For this data set, we have a set of the abbreviation and real name of each MBTA station from MBTA GitHub.
4. presidentElectionByPrecinct.py
  For this data set, we retrieve how many voters participate in presidential election in 2016 by precinct in each city from http://electionstats.state.ma.us.
5. bostonPopulation.py
  For this data set, we collect how many people lived in Boston in 2011 and population estimates in 2016 from US Census.
6. pollingLocation.py
  For this data set, we collect the information of polling location from Analyze Boston.

## Data Transformation

We performed three transformaion to process retrieved data sets to new three data set.

1. Station_Node
  We processed averageDelay and Station_Node by using projection, selection and aggregation to combine these two data sets into one, because our “averageDelay” dataset only contains the abbreviations of each stations in Boston area, and our “Station_Node” dataset stores the abbreviations paired with the stations’ full name. After running the merge method, we get a final dataset which contains the full names of stations and the delay time between each of them.

2. mergeVotePopulation
  To calculate how many Bostonian was available to vote, we selected and projected Population Estimates and the number of person under 18 from bostonPopulation data set. Then, we selected only Boston area from presidentElectionByPrecinct data set and do summation of the number of voters. With processed data from two data set, we get new data, voter turnout.

3. countPollingLocationByWard
  To find the relationship between public transportation and voter turnout, we have to find the accessibility of polling location in Boston. Therefore, we count how many polling locations are in each ward. After finding more geojson data of Ward and Precinct in Boston, we will combine geojson data to countPollingLocationByWard.

## To Run this Project

Before running this project, we checked that we can run each file by itself very quickly. Also, without mergeVotePopulation.py, running execute.py file does not take a long time. If running the following line takes a long time, please move mergeVotePopulation.py file in different place and run execute.py file. Then, run mergeVotePopulation.py file after uncommenting the last four lines.
```
python execute.py yjunchoi_yzhang71
```
