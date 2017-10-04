
Authors:

  Jennifer Liang: jliang24 
  
  Taylor Potye: tpotye 

Purpose:

The city services of Boston have a wide range of area to cover, but are all the areas covered equally? We are trying to discover if there is a discrepancy in the services provided by Boston based on location value. We are evaluating whether the areas with the higher property value have a high quality of services compared to the areas with lower property values. The areas of service we chose to focus on are the number of hospitals, number of police stations, and the number of service requests being serviced (specifically pothole repairs) per zip code. To begin to answer our question we are using this data and comparing it to the property value of each zip code. 

DataSets:

  Property Assessments from 2016 from City of Boston data portal
  
  Boston 311 Service Requests from City of Boston data portal
  
  Pothole Repairs from National League of Cities data portal
  
  Hospital Locations from Analyze Boston data portal
  
  Boston Police Stations from City of Boston data portal

Transformations:

aggregateProperty- Using the Property Assesments data, through a series of aggregations, projections, and unions this transformation finds the average property value per zip code.

potholeAnalysis- Using the Boston Police Station and Hospital location data, through a series of selections, aggregations, projections, and unions this transformation determines how many police stations and hospitals there are within each zip code.

policeHospital- Using the Boston 311 Service Requests and Pothole Repairs data, through a series of unions and aggregations this trasformation gives the ratio of pothole service requests to pothole repairs per zip code.  

Required Python Libraries:

  dml
  
  prov
  
  numpy