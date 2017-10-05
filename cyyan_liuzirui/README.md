
Find the most suitable area for building schools in Boston

Boston, as one of the Boston metropolises of the United States, is especially famous for its education. There are lots of schools in Boston and a great number of students from all over the world. Therefore, how to find the most suitable area for building schools becomes an important issue. To solve this issue, we have to consider if there are good infrastructures and facilities for students near schools. To completely solve this problem, we have to consider many aspects.

In this project, for the sake of simplicity, we only consider four main factors including trash receptacle locations, gardens, police stations, and hospitals. The data sets we are using to support this goal are the following:
1. Boston Public Schools (School Year 2012-2013) https://data.cityofboston.gov/resource/492y-i77g.json (our main dataset)
2. Boston Police District Station https://data.cityofboston.gov/resource/pyxn-r3i2.json
3. Hospital Locations https://data.boston.gov/export/622/208/6222085d-ee88-45c6-ae40-0c7464620d64.json
4. Big Belly Locations https://data.boston.gov/export/15e/7fa/15e7fa44-b9a8-42da-82e1-304e43460095.json
5. Community Gardens http://data.cityofboston.gov/resource/rdqf-ter7.json

The way we want to implement these data sets is to evaluate the distance between schools and these infrastructures and facilities. If gardens, hospitals, police stations, and trash receptacles are concentrated in a certain area, we consider this area is suitable for building schools. If an area does not have many gardens, hospitals, police stations, and trash receptacles, we consider this kind of area is not suitable for building schools.

To reach our goal, we implement three transformations:
1. We merge Boston Police District Station and Hospital locations to calculate the number of police stations and hospitals in a certain zipcode
   -We first use projection to get the zipcode of police stations and hospitals, repectively
   -Then, we use aggregation to calculate the amount of hospitals and police stations in each zipcode
2. We merge Boston Public Schools and Community Gardens to calculate the distance between each school and each garden
   -First, we use projection to get the coordinates of schools and gardens, respectively
   -Then, we use two "for loops" to calculate the distance between schools and gardens
3. We merge Boston Public Schools and Big Belly Locations to list all big belly within 3 miles from each school
   -First, we use projection to calculate get the coordinates of schools and big belly, respectively
   -Then, we use two "for loops" to calculate the distance between schools and big belly
   -Finally, we list all the bigbelly within 3 miles from each school. The reason why we choose 3 miles is that if the big belly(receptacle locations) are too far away from schools, it will not be very convenient for students and school teachers to drop litters.




