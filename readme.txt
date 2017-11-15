Problem:
<<<<<<< HEAD

Looking at charging stations, hubway stations, biking networks, and open space data, we first grouped all of the data by neighborhood, essentially computing a score for each neighborhood. By grouping this data together, we can start to look at solving problems such as finding out if there exists any correlation between certain subsets of our data such as number of charging stations and hubway stations, or charging stations and bike networks, or charging stations and open spaces, or hubway stations and biking networks, or hubway stations or open spaces, or biking networks and open space data. We are looking for correlations across neighborhoods, and whether or not there exists a correlation between placement of certain items across neighborhoods. Therefore, we iterate through all the possible subsets of two items and look for correlations. 
	Furthermore, we are also trying to find out given constraints for the minimum number of items we can place  in total, and a $1000000 budget for each neighborhood, whether or not there exists a solution that satisfies these constraints within the budget and how many items we end up placing in each neighborhood. Essentially, we want
























Expanding public transportation and car sharing are two of the most popular solutions to reduce emissions produced per individual. However, these options are not necessarily zero emission options and still pollute the enviornment. Therefore it's important that more effective measures be considered to reduce emissions for individual commutes to zero. The solutions we are considering are biking, and charging stations for electric cars which all contribute zero emissions assuming electricity powering them is also generated in a enviornmentally friendly way. Using Hubway data, bike network data, charging station data, and neighborhood data we will attempt to find the most optimal "green score" for each neighborhood within Boston. Our definition of a "green score" for a neighborhood will be the proximity and frequency of zero emission forms of transportation. Initially using various projection and selection transformations we will calculate the green scores for each neighborhood. Then we will use k-means to find more optimal placements of these zero emission forms of transportation. This will allow us to find the most optimal "green score" for each neighhorhood. 
=======
Expanding public transportation and car sharing are two of the most popular solutions to reduce emissions in urban environments. However, these options are not necessarily zero emission options and may still pollute the environment. Therefore it's important that more effective measures be considered to reduce emissions. 

By looking at charging stations, hubway stations, biking networks, and open space data in each Boston neighborhood we determined a green score for each neighborhood. From here we created a statistical analysis in an attempt to find out if there exists any correlation between subset entities of our data and if these correlations corresponded to the number of placements of select entities in each neighborhood. To do this, we iterated through all the possible subsets of two entities within neighborhoods and calculated correlations.

Finally we took the green scores we computed initially and set up a constraint satisfaction problem where we attempted to optimize the green score for each neighborhood given a budget of $1,000,000. The constraints we added attempted to make the computation realistic, meaning that solutions of 0 or less than 0 were not acceptable, in an effort to deplete the budget.We also randomized the maximum and minimum number of specific entities that could be built in neighborhoods in an effort to create a unique solution for each neighborhood. 

Notes:
Our Z3 files are modified slightly:
For Z3core, Z3printer and Z3 we removed the ". import" lines from the header.
>>>>>>> 54b0d0c7a9013ae250ac8d740b57ac107736517f
