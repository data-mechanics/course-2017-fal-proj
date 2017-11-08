Authors: Eric Jacobson & Andrew Quan 
Class: CS591 Data Mechanics
Instructor: Andrei Lapets

The Project Overview: 
Growing up in the suburbs of Boston, Eric and Andrew have always been interested in the safety of their city. By studying data sets containing information about crime reports, firearm recovery, housing violations, motor vehicle violations, and 911 call totals, we hope to gain insight on law violation patterns in Boston and its neighboring boroughs (Cambridge & Somerville). Our goal is to find a correlation between the locations of non-violent crimes (such as vehicle and housing violations) and potentially violent crime (areas where firearm recovery occurred and crime incident reports). By establishing a link between the two, law enforcement could move their focus to these areas in the hopes of preventing crimes before they occur. If we are able to connect the 911 call log breakdown by department to certain areas, we could further focus where the police, fire, and emergency medical services should be stationed. If our findings are tracked for a period of time this could lead to the establishment of new police, fire, and EMS stations in optimal locations across the city of Boston.


How to Use:
-All our resource files are public and do not require authentication. 
-In bash, within the directory of aquan_erj826, run 
python3 execute.py aquan_erj826
-This will go into the aquan_erj826 folder and run all the execute and provenance functions. 
-In the same directory, you can now view ‘provenance.html’ to see how the different entities were derived via activities by the agents in our project.


Resources: 
1. Firearm Recovery List in Boston
http://data.cityofboston.gov/resource/ffz3-2uqv.json

2. 911 Dispatch Calls in Boston
https://data.boston.gov/export/245/954/2459542e-7026-48e2-9128-ca29dd3bebf8.json

3. Crime Incident Reports in Boston
https://data.boston.gov/datastore/odata3.0/12cb3883-56f5-47de-afa5-3b1cf61b257b?$top=1&$format=json

4. Housing Violations in Cambridge
https://data.cambridgema.gov/resource/bepf-husa.json

5. Motor Vehicle Citations in Somerville
https://data.somervillema.gov/resource/jpgd-3f23.json


Transformations:
crimeDispatch.py
This transformation makes use of selection, projection, and aggregation. With the 911 calls data, we first selected the values from 2014, then projected the dates as keys and set the values to be the total calls from that date. In the crime data, we then isolate the dates of each crime incident reported. We subtract 1 from the totals of 911 call collection on a given day for each crime incident reported on that day. This gives us a resulting dataset with dates as keys and the total number of 911 calls that did not result in a crime incident report as the values. The new dataset is of the form:
{date:total_calls}

gunsRecovered.py
This transformation makes use of projection. We utilize each date, and the sum of all the gun recovery totals to create our new dataset whose objects are of the form:
 {'DATE':date, 'TOTAL_GUNS_COLLECTED':total}

vehicleAndHousingCitations.py
This transformation uses projection and aggregation. We utilize each date, time, type of violation, and specific charge as the fields of our new dataset. We aggregate the two sets together to yield a master list of motor vehicle and housing citations in Boston burroughs, with several fields removed using projection of the form:
 {{'DATE':date, 'TIME':time,'TYPE':kind, 'CHARGE':charge}}
