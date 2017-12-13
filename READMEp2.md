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