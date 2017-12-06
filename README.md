# BU CS591 Fall 2017 Project

**Team Members:**
+ Mengyao li
+ Zhuoshu Yang

## Introduction
In this project, we want to figure out the relationship between hospital and garden. Nowadays, people like sports outside in Boston. Many people run or ride bikes in the park or garden; however, people are easily to get hurt from sporting. It would be necessary and convenient for them if there are hospital close to the garden and park. On the other hand, patients in hospital can talk a walk or relax if there are gardens or parks near the hospitals. We sperate the map of Boston in terms of the zip code and examine the if the area has enough hospitals for people when they do sports in garden and park. We find data about the number of hospitals and gardens in Boston area represented by zip code from data.boston.gov. Through our algorithm, we will find which area do not have plenty of hospitals and a optimized place a build a new hospital. By displaying this infomation, we can come up more practical solution in building hospital for the people in Boston.

## Datasets
Edited file: hospital_garden_transformation(edit), neighborzipcode(new implement), insufficient(new implement), addHospital(new implement)

In hospital_garden_transformation, we merged the number of hospitals and number of gardens in specific area. Then we create a new dataset of dictionary with the key of zip code and value of list of adjacent zip code). (e.g {"02136": ["02136", "02132", "02131", "02126"]). The constrain we added is the number of hospitals within the adjacent area divide the number of garden in this area. If the output is less than five, it need more hospital.
	By approaching this algorithm, We iterate the merged datasets(number of hospital and garden) twice and neighbor dataset once to find the ratio of hospital and garden. 
	Now we generate a new dataset containing the zip code with insufficient hospital with its ratio. Later in addHospital file. We implement the algorithm to find the optimal place to build a new hospital. We just iterate the dataset ratio to add a new hospital to this zip code and then find the total score(sum of ratio). Then the zip code with the highest score is the place to build a new hospital. Then we store the updated data set as addHospital in mongo.

Further development:
	If we wanna build n more hospitals. we can simply run the algorithm n times and replace the original dataset with updateed datasets. 
