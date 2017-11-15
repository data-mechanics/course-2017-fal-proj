Problem:
Expanding public transportation and car sharing are two of the most popular solutions to reduce emissions in urban environments. However, these options are not necessarily zero emission options and may still pollute the environment. Therefore it's important that more effective measures be considered to reduce emissions. 

By looking at charging stations, hubway stations, biking networks, and open space data in each Boston neighborhood we determined a green score for each neighborhood. From here we created a statistical analysis in an attempt to find out if there exists any correlation between subset entities of our data and if these correlations corresponded to the number of placements of select entities in each neighborhood. To do this, we iterated through all the possible subsets of two entities within neighborhoods and calculated correlations.

Finally we took the green scores we computed initially and set up a constraint satisfaction problem where we attempted to optimize the green score for each neighborhood given a budget of $1,000,000. The constraints we added attempted to make the computation realistic, meaning that solutions of 0 or less than 0 were not acceptable, in an effort to deplete the budget.We also randomized the maximum and minimum number of specific entities that could be built in neighborhoods in an effort to create a unique solution for each neighborhood. 

Notes:
Our Z3 files are modified slightly:
For Z3core, Z3printer and Z3 we removed the ". import" lines from the header.