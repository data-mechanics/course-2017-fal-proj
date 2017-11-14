Authors:

Ivan Wong : wongi

Purpose:

I want to discover whether there is a correlation between the amount of development in a neighborhood to its cost of living. In doing so, I looked for number of hospitals, streetlights, public schools (boston and cambridge), and checked it relative to the cost of property in each zipcode. I aggregated these elements based on zipcode.

DataSets:

Property Assessments from 2016 from City of Boston data portal

Cambridge Schools from City of Cambridge data portal

Boston Streetlights from Analyze Boston

Hospital Locations from Analyze Boston data portal

Boston Public Schools from City of Boston data portal

Transformations:

aggpropValue - Aggregates properties based on zip code, and average the property values. Utilizes aggregation, projection, and union.

camSchoolsAgg - Aggregates schools in Cambridge based on coordinates, converting coordinates into zip codes, and aggregating.

schoolsAgg - Aggregates Boston Public Schools based on zip code, utilizing aggregation and projection.

hospitalAgg - Aggregates hospitals in Boston based on zip code, utilizing aggregation and projection.

lightCoordinates - Aggregates streetlights in Boston based on coordinates, converting to zip code, utilizing aggregation and projection

completeAgg - Aggregates all of the data. Projects into tuples of (zipcode, (key value)), aggregates based on zipcode.


Required Python Libraries:

dml

prov

numpy

geopy.geocoders

defaultdict

