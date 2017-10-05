# CS 591
# Data Mechanics
# Andrei Lapets

# Authors
-Alan Burstein 
-Joseph Caluag 

#Problem Statement
Boston roads and their problem areas

	Vision Zero is a safety concern application where citizens can enter complaints on road safety and road maintenance issues in Boston. The dataset provides the type of complaint, the date of the complaint, and the geographical coordinate of the problem area. With Vision Zero, we can locate the roads in Boston that require attention.

	The Vision Zero dataset was extracted, along with MBTA bus stations, traffic Signals, bike routes, and Hubway stations. All of these datasets contain geographical coordinates. With an overlay of all the datasets, we obtain a map through which we can learn more about the areas that have problems. Such a map provides a means of ranking the complaints based on priority. For example, a problem area that is near a bus station and many traffic signals would indicate a busy intersection. Government workers can then quickly identify the areas they want to allocate their time and resources to. 
	
	We are then able to see changes over time in the complaints and see areas that are consistently problematic and see the effect of routes on those areas.	

#Run Instructions
- Run execute.py
- mbta.py requires authentication. auth.json must contain an MBTA API key in the form {"MBTA_api_key":<key>}. 

#Transformations
roadComplainAgg: Filter the road complain data to get only relevent data. Next aggregate data by date and get a list of all complaints on that day through a projection 

projectData: Project the trafficSignal, mbta, and hubway datasets to remove irrelevant data and reorder the keys

filterHubway: Filter hubway data (projected) by removing data points outside of Boston

transportStops: merge MBTA and Hubway datasets

bikeNetwork: Project the userful bikeNetwork data while reading it in

#Resulting dataSets

alanbur_jcaluag#roadComplaintsByDay
alanbur_jcaluag#transportStops
alanbur_jcaluag#trafficSignalProjected
alanbur_jcaluag#bikeNetwork

