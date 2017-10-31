# Optimal Placement of Speed Feedback Signs

I have retrieved and combined datasets to find optimal locations to place speed feedback signs in Boston (inspired by a Vision Zero’s Data Challenge: https://www.boston.gov/calendar/vision-zero-data-challenge-final-presentations).  I retrieved datasets with locations of parks, schools, and elderly homes, because these areas are more vulnerable and more likely to have many pedestrians or people who might not look before crossing the street. I also queried crime reports for motor vehicle accidents. I combined the accident dataset with a dataset of streets and their speed limits, because it might be optimal to place speed feedback signs on streets with many accidents and high speed limits.

Also, Vision Zero has an app where people can submit suggestions regarding safety issues (http://app01.cityofboston.gov/VZSafety/), and I queried that dataset for complaints about people speeding. I then combined this dataset with the locations of parks, schools, and elderly homes to see which complaints had coordinates within half a mile of those sites. I also ran k means to create two clusters of complaints. In the future, I would take into account which locations had the most complaints, as well as which complaints were closest to the vulnerable sites I mentioned earlier, in order to determine where speed feedback signs should be placed.

To Run:
* Some of the url's I requested did not have appropriate suffixes to put in the doc.entity part of the provenance funciton, so I generated a unique uuid in that case, as suggested in lecture.
* You will need to import the "requests" and "geopy" libraries

Datasets:
 * Boston open spaces: http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson (from ArcGIS Open Data)

 * Boston Segments (for speed limits): http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson (from ArcGIS Open Data)
 * Boston Elderly Housing: http://services.arcgis.com/sFnw0xNflSi8J0uh/ArcGIS/rest/services/ElderyHousing/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Project_Name%2C+Housing_Type%2C+Parcel_Address%2C+MatchLatitude%2C+MatchLongitude&returnGeometry=true&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnDistinctValues=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token=  (from ArcGIS REST service)
 * Boston Public Schools: https://boston.opendatasoft.com/api/records/1.0/search/?dataset=public-schools&rows=-1 (from Boston Wicked Open Data)

 * Car Accidents in Boston: https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000 (from data.boston.gov)
 * Vision Zero Entries: https://data.boston.gov/api/3/action/datastore_search?resource_id=80322d69-c46f-4b93-9c38-88e78ae59a34&q=people%20speed&limit=5000 (from data.boston.gov)

Transformations:
* merge_accidents_speed_limits.py: For each street, get speed limit and number of accidents
* find_nearby_sites.py: For each speeding complaint submitted to Vision Zero, see if their is a school, park, or elderly home within half a mile of where the complaint refers to
* cluster_complaints.py: Run k-means on complaints submitted to Vision Zero
