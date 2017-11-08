Objective:
Since Boston is a quite densely populated area in US, here will be many people and travelers. We are exploring the lifestyle in Boston. We would like to see whether a place is suitable for people to enjoy their lives around Boston. It is useful for both travelers and local people. It saves people a lot of times to ask their friends where is suitable and safe to visit. If people have some medical problem, they can also visit nearby medical centers. 

Databases:
1. Crime: https://data.cityofboston.gov/resource/ufcx-3fdn.json
2. Hospital: https://data.cityofboston.gov/resource/u6fv-m8v4.json
3. Entertainment: http://datamechanics.io/data/eileenli_yidingou/new.json (Originally from:https://data.boston.gov/export/792/0c5/7920c501-b410-4a9c-85ab-51338c9b34af.json)
4. Health:http://datamechanics.io/data/eileenli_yidingou/Health.json
5. Restaurant:http://datamechanics.io/data/eileenli_yidingou/Restaurant.json


Combining Data:
1. Ent_Crime: we use the databases Entertainment and Crime for merging. We extracted the businessname and coordinates from Entertainment and coordinates from Crime. We calculate the distances from business and the place where crime takes place and check whether the distance is less or equal to 1.5 miles. If it is, counts it under the key businessname in the new merged database. We can check if the entertainment’s surrounding is safe or not for people to visit.

2. mergeEHzip_name: We use the databases Entertainment and Hospitals for merging for this part.We extract the zip code and businessname from Entertainment and extract the zip code and hospital name from Hospital database. Check if their zip code matches and if they match we will put them into a new merged database under the key zip code. This basically helps people to check whether there is a hospital nearby an entertainment. If they injury, they can go there immediately, especially for travelers who are not locals.

3. HealthRestaurant:We use the database Health and Restaurant for merging this part. We get the coordinates and Businessname from Restaurant and the coordinates and health center name from health database.With the coordinates of health center and restaurants, we can find the number of restaurants within walking distance of a hospital, which we believe is 1.5 miles. This dataset can help people find some restaurant after they go to health center, or visiting a patient.
 