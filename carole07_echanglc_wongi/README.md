Authors:

Carole Sung : carole07
Eric Chang : echanglc
Ivan Wong : wongi


Purpose:

We want to score each zipcode by the ratio of government developed structures to cost of living. If there is a higher score, that indicates there is too few developed structures as compared to the cost of a home in the area, pointing to a greater need for government development. 

Write-up:

We hope to use our data of police stations, hospitals, schools, streetlights location, and property value to indicate places for optimal government development. In order to do this we are going to need to determine a development score. The development score will be taken by the average cost of home divided by government development(police stations, hospitals, schools, and streetlights) in a zip code. If the value is low then the average cost of home is small or there is a relatively significant amount of government development. Because if the value of cost divided by development is high then there is an optimal opportunity for government development. However, if the value is low due to there being more development in comparison to property value, then there is less need for government development. 




DataSets:

Property Assessments from 2016 from City of Boston data portal

Cambridge Schools from City of Cambridge data portal

Boston Streetlights from Analyze Boston data portal

Hospital Locations from Analyze Boston data portal

Boston Public Schools from City of Boston data portal

Boston Police Stations from Analyze Boston data portal

Transformations:

aggpropValue - Aggregates properties based on zip code, and average the property values. Utilizes aggregation, projection, and union.

camSchoolsAgg - Aggregates schools in Cambridge based on coordinates, converting coordinates into zip codes, and aggregating.

schoolsAgg - Aggregates Boston Public Schools based on zip code, utilizing aggregation and projection.

hospitalAgg - Aggregates hospitals in Boston based on zip code, utilizing aggregation and projection.

lightCoordinates - Aggregates streetlights in Boston based on coordinates, converting to zip code, utilizing aggregation and projection

policeAgg - Aggregates police stations in Boston based on zip code, utilizing aggregation and projection.

developmentScore - Aggregates all of the data. Projects into tuples of (zipcode, developmentCount), aggregates based on zipcode, then calculates development score by arbitrarily dividing the average property value by the number of buildings nearby, then normalizing the values to a score of 0-100.


Required Python Libraries:

dml

prov

numpy

geopy.geocoders

