###### Tackling Obesity in Boston - Max Biel & James Otis

## Introduction: 
```
    Obesity is one of the United States largest health problems.  Several subproblems have been attributed
    to the rise of obesity - many of which point to a root cause, which is inaccessability to healthy and
    nutritious food options.  Our goal throughout the semester was to use publicly available data to make
    healthy options more accessible to those who are considered obese under the Massachusetts medical
    standards.
    
    At a high level, there we're two portions of our project: 

    (1) Analytics 
    (2) A solution to an optimization problem.
    
```
## Analytics:

###### Methods:
```
    In the analytics portion of our project, we wanted to measure the relationship between income and 
    obesity. In order to do this, we combined data from the following sources:

    (1) Property Values in the Boston Area:
        The columns we used from this dataset included the total value of the associated property,
        and the coordinates.

    (2) Obesity Data:
        The columns used from this dataset included the coordinates of the associated obese person.

    (3) Boston Zoning GeoJson:
        This is a shapefile used to determine if a coordinate falls within a partiular neighborhood.

    With the above information, we we're able to associate each obese person to one or multiple properties
    based on proximity.  The resulting transformation was of the form: (Property Value: Number of obese 
    people within close proximity).
    
    The next transformation effectively "binned" each property value into a range of property values
    (i.e values ranging from $100,000.00 - $110,000.00), and aggregate the associated number of obese
    people as the value. More specifically, the transformation took the following form:
        (Value Range: Number of obese people associated with range)

    The above transformation was run for each neighborhood, as well as for the entire Boston
    area.  At this point, we we're ready to compute the correlation coefficient for each neighborhood
    in Boston, as well as the entire area.
```

###### Expected Results:
```
    The results of the analytics phase we're meant to play a role in the optimization problem we wanted
    to solve.  For example, if there we're much higher correlations coefficients in certain neighborhoods,
    we would be able to create solutions that prioritized those areas. We expected to find a positive 
    correlation between lower valued areas and the number of obese people in the area, like the following
    image illustrates:
```

<img src="https://github.com/jmbiel/course-2017-fal-proj/blob/master/BIEL_expected_results.png">


###### Actual Results:
```
    The purpose of our project was to gather data on overweight persons in the Boston city area,
    and calculate whether or not there exists a correlation between income/property values and 
    the number of overweight people in then vicinity. Furthermore, we gathered data on the 
    location of current winter food markets and data on failed health inspections and their 
    corresponding locations. This allows us to map/plot the locations thathave poor health 
    standards and access to safe/healthy food. These two data points allow us to make a 
    constrained decision of the optimal location of health food stores. Our findings are 
    constrained to just the Zipcodes that are registered with the city of Boston (I.E Brookline 
    is not included in our findings). Our data points are further constrained to just landmass, 
    as optimal locations could indicate a non-viable placement for a store/restaurant.
    
    Part II:
    
    For part II of the project to apply constraints, we first added constraints of the location
    of each obese person to the market was limited to a distance of under 1 mile. The second 
    constraint we used first required that we limit the shapefile of the Boston Area to just 
    land mass (Couldn't remove ponds and lakes/rivers). We then calculated the optimal locations
    of health food markets and if they were not in the shapefile, then they were adjusted to be
    within the location constraints of the shapefile. Our findings indicated that there is a need
    for more healthy foods in the areas of Dorchester/Roxbury (Some market locations were hilariously
    placed around KFC/McDonalds/Taco Bell and other fast food stores). The data also indicated that 
    there was less of a need for Healthy food stores in the financial district and the Allston - 
    Brighton areas. This is conclusive with our findings of the correlation between income
    and obesity. 
        Our second constraint was to find the correlation of obesity and property values, 
    with a subset that allows us to compute the correlation coefficient between the neighborhoods
    in the shapefile and the overweight individual/property values from our other datasets. This 
    data allowed us to conclude that HuntingtonAve/Prudential Center, Bay Village Neighborhood, Fenway 
    Neighborhood, Government Center and Central Artery had no major correlation between
    obesity and property value, while Charleston, and the South End districs had a significant,
    measurable correlation between the two metrics.
```