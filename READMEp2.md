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
<p align="center">
    <img src="https://github.com/jmbiel/course-2017-fal-proj/blob/master/heatmap.PNG?raw=true">
</p>

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
        This is a shapefile used to determine if a coordinate falls within a particular neighborhood.

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
<p align="center">
    <img src="https://github.com/jmbiel/course-2017-fal-proj/blob/master/BIEL_expected_results.png">
</p>

```
    Given these results, we would have the ability to further segment our obesity data and potentially
    specialize our solutions for each variant of the problem.
```
###### Actual Results:

```
    The correlation coefficients per neighborhood seemed to show no statistical correlation between
    income/property value and obesity.  In some neighborhoods, we saw correlation coefficients close
    to 0, and in others we saw corrleation coefficient closer to -0.5 with a high p-value, indicating
    that we could not be confident in accuracy of the analysis performed. These can be potentially 
    attributed to a lack of obesity neighborhood in each neighborhood.  This analysis would certainly
    be more accurate if we also had data about the number of healthy people in each neighborhood.
    Then, we could cross-reference our results with another analysis which correlates income/property value
    with the number of healthy people in each neighborhood.

    Regardless, with a lack of statistically significant evidence to support our hypothesis, we decided
    to proceed with a general solution to an Obesity problem for all of Boston based on our obesity dataset.
```
## Optimization Problem:

###### Overview:
```
    Given the statistical analysis, we decided to create a general solution to the problem of
    obesity in Boston.  We concluded that the part of the larger issue at hand is the 
    inaccessability of health food to people accross Boston.  We wanted find a way to 
    optimally place healthy food stores around Boston to make healthy food more accessible. 
```

###### Problem to Solve:
<p align="center">
    <img src="https://github.com/jmbiel/course-2017-fal-proj/blob/master/optimazation_prob.PNG?raw=true">
</p>

###### Methods:
```
    We knew that we wanted to use K-Means to place health-food stores in an optimal manner.
    The obesity dataset was our primary stream of data for the points to place means around.
    However, we added a couple of constraints that would change the raw output of k-means.

    First, we added the constraint that the AVERAGE distance of an obese person to a health-
    food store must be less than one mile.  The second constraint we added stated that health-
    food stores must be realistically placed in Boston (i.e. not under a bridge/in the water).
    Lastly, we wanted to minimize the number of means needed to satisfy the above constraints.

    We solved this problem by running k-means in a loop, starting with one mean and incrementing
    the number of means with each iteration until the constraints we're satisfied. Each time
    k-means executed, we would replace the means according to the Boston Zoning shapefile which
    would ensure that the means we're placed realistically.  The shapefile contained the
    appropriate borders which would satisfy our constraint.  If a mean fell outside of the
    shapefile, we would place it to the closest point on the shapefile.  After this, we calculated
    all the distances of obese persons to the means.  If the average was greater than one mile,
    we incremented the number of means and re-ran K-Means.

    The run-time of this algorithm is not impossibly slow.  The amount of points we are using yields
    a run-time with an acceptable termination time.  It takes roughly 3-4 seconds to calculate the
    means that satisfy the constraints.  However, with many more points, this algorithm would become
    very slow.  Something to consider for future is a way to make this scalable -- for example
    potentially distributing the distance calculations and the replacement of each mean.  With this
    being said, the bottle-neck is most certainly K-Means.  The solution for now is to run this algorithm
    in a relatively small area such that the run-time is acceptable.

    The final output would have the locations of all the health-food stores, with all the constraints
    satisfied. 
```

###### Output Picture:
<p align="center">
    <img src="https://github.com/jmbiel/course-2017-fal-proj/blob/master/k-means-ouptut.PNG?raw=true">
</p>

```
    NOTE: Our solution DID NOT include the Cambridge area.  Only neighborhoods that we're included
    in the Boston Zoning shapefile.
```

###### Conclusion:
```
    We believe that our project was successful in optimally placing health-food markets around the
    Boston area using data centered around obese persons. There are several expansions that could
    be made (covered in next section) to create an all-encompasing solution with a much higher
    potential for impact.
```

## Future Work:

###### Incorporating More Data:
```
    More data could be included to cross-examine the correlation coefficients.  For example, it would
    be useful for the sake of accuracy to have a dataset comprising of all healthy people and their
    respective locations.  Then, we could run the correlation coefficient between healthy people and
    property value per neighborhood and see if it makes sense compared to the coefficients computed
    between obesity and property value.

    Also, an income dataset that has corresponding information with respect to location.  This would
    serve as a more accurate feature to use for the purposes of computing correlation coefficients.
```

###### Expanding the Context:
```
    This solution can be expanded & used in many different contexts.  For example, the same general
    principles can be used to optimally place gyms.  With a different dataset containing points relating
    to ill persons, the algorithm can be used to optimally place hospitals.  The solution is generally
    designed such that it will always minimize number of means utilized, and satisfy some objective
    function (in our case distance).
```



###### Tackling the Economical Component:
```
    Obviously optimally placing health-markets is only a partial solution to making nutritious food
    more accessible.  The other part of the solution is the ability to make this food affordable,
    which is a much more business/economic problem to solve.  Our solution could be expanded to provide
    optimal pricing of health-foods based whole-sale prices, and the average supply chain cost of a large
    scale produce (such as Amazon or Target).  I specifically mention large scale supplier because they
    generally have a greater capability to innovate their supply chain & profitibility models. 
```

## Appendix:

###### Datasets Utilized throughout the Duration of the Project:

1. #### getHealthInspections.py 
```
    Retrieves data about HealthInspections from data.boston.gov and deposits the data in an instance of MongoDB.
```
2. #### getObesityData.py 
```
    Retrieves 16000 data points regarding obesity statistics from the CDC website.
    This is then added to the instance of MongoDB for further manipulation downstream.
```
3. #### getOrganicPrices.py 
```
    Retrieves data of the average price for particular food items
    during the 12 months of the year. This data is not used downstream.
```
4. #### getPropertyValues.py 
```
    Retrieves data on different types of buildings/homes in boston as well
    as other descriptors of value from data.boston.gov. This data is used
    downstream for a correlation between obesitry and income.
```
5. #### getWinterMarkets.py
```
    Retrieves data on the location of seasonal markets in the boston area from data.cityofboston.gov.
    This data is deposited into the MongoDB instance and is used downstream to calculate the distance 
    of optimal placements of markets based on obesity statistics.
```
6. #### getZipCodeData.py
```
    Retrieves a mapping object of the zipcodes in Boston to their respective districts 
    and town names. This is used downstream as a metric of joining some datapoint 
    fields based on ZipCodes.
```
7. #### setHealthPropertyZip
```
    Retrieves the data generated by getHealthInspections.py, getPropertyValues.py, 
    and getZipCodeData.py and runs multiple aggregations, projections, selections, 
    and transformations on the data to properly format the data to place back in mongoDB.
```
8. #### setObesityMarkets.py 
```
    Retrieves the data generated by getObesityData.py and getWinterMarkets
    and runs K means to determine the optimal location for Winter Health Markets.
```
9. #### getBostonZoning.py
```
    Retrieves a GeoJson of the landmass of the Greater Boston and area and deposits
    it in MongoDb.
```

10. #### setObesityPropertyCorrelation.py
```
    Reads the datasets on Obesity and Property Values and first maps each overweight 
    person to the properties around them. The amount of overweight individuals to each 
    property is then condensed into ranges (incremented by 100,000). This bucked data is 
    then used to calculate the correlation coefficient of obesity to property values and 
    the output is loaded into MongoDB. Our findings showed the expected results, that there 
    are more overweight people in areas with lower property values.
```

11. #### setOptimalHealthMarkets.py
```
    Runs k-means to determine the optimal locations for Health Food stores in the Boston
    area based off of the location of overweight individuals in the Boston Area. 
    Constraints are also applied from a shapeFile that is limited to the Boston
    Area landmass.
```