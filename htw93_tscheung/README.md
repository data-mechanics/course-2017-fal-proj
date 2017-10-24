# Project Report
## Members
**Haotian Wu**, **Desheng Zhang**

## Project Narrative
Our project seeks to exlpore the relationship among shcool location, crime and hospitals in Boston and New York. We believe that there's some relationship like the number of crimes within one or two miles of the school.

## Dataset Description
1. **Schools in Boston:** This dataset contains most school and their locations in Boston.
2. **Boston Crime Data:** This dataset contains the crime, crime type and location of crime in Boston.
3. **Boston Hospitals Data:** This dataset contains the location and name of the hospitals in Boston.
4. **Schools in NewYork:** This dataset contains most school and their locations in New York.
5. **New York Crime Data:** This dataset contains the crime, crime type and location of crime in New York.
6. **Boston Hospitals Data:** This dataset contains the location and name of the hospitals in New York.

## Transformations

There are four transformations, which are **BostonSchoolsCrime**, **NewYorkCitySchoolsCrime**,**BostonSchoolsHospitals** and **NewYorkCitySchoolsHospitals**.

We calculate the number of Crimes and hospitals near schools in Boston and New York, select meaningful features from each collection and combine them into new collections. For example, we calculate the number of crimes within a mile of each school, and combine two collection into new collection with name of school, location of school, location of crime, description of crime, etc.