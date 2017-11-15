# angelay_maulikjs Project 2

This project is a continuation of angelay's project 1.

getData.py retrieves the 7 datasets <br />

merge2012.py merges all the data from 2012 and puts them into a collection called all2012. There are 129 entries in all2012. <br />

merge2013.py merges all the data from 2013 and puts them into a collection called all2013. There are 129 entries in all2013. <br />

removeOutliers.py takes the data in all2012 and removes all entries with outliers. This leaves us 87 entries left and stored in collection clean2012. <br />

removeOutliers2013.py takes the data in all2013 and removes all entries with outliers. This leaves us 87 entries left and stored in collection clean2013. <br />

correlations.py takes all the data from clean2012 and returns the correlation coefficient of CO2 emissions and all 6 other attributes: carbon intensity, energy intensity, energy use, GDP per capita, Human Development Index, and population. The results are stored in collection corr2012. <br />

statsmodel.py runs the statistical analysis and runMachineLearning.py runs the machine learning.

In project 2, we are trying to build a model that will predict a country's CO2 emissions given its population, GDP per capita, carbon intensity, energy intensity, energy use, and HDI. We took two different approaches to solve this problem: machine learning and statistical analysis. <br />

In statsmodel.py, we performed statistical analysis and built our model by adding one independent variable at a time, in descending order of the R-squared value of the linear model of CO2 emissions with only that variable. We based our methodology on the Kaya Identity, which states that CO2 emissions is equal to population * GDP per capita * carbon intensity * energy intensity. We added energy use and HDI to optimize our model. <br />

We first built our model with clean 2012 data without outliers and tested it on clean 2013 data without outliers, and obtained an R-squared value of 0.984442. Then we built our model with all 2012 data with outliers and tested it in all 2013 data with outliers, and obtained an R-squared value of 0.998919. When running statsmodel.py, it will produce three graphs saved in the same directory, the first one is the pair-wise correlations between all variables, the second one is the results of the model on clean data, and the third one is the result of the model on all data. <br />

In __Machine Learning__ we are using a Multilyer Perceptron Network to try and fit the model to the given data with regularization:0.01. (To prevent overfitting). We used n^5 hidden layers where n is the number of variables. The model runs for max 1000 iterations which gives us a good enough model given some margin of error but it hasnt converged yet. It has an inital learning rate of 0.001 and a momentum of 0.4 <br />

We first scaled the input using StandardScaler and then transformed both the training and testing inputs using the same scaler we used to scale the training data. <br />
We then trained the model on the training set and used the testing set (clean2013 data) to predict the output.<br />
We compared the predicted values from the model to the known output values of clean2013. <br />
The model yields an R-squared value of 0.9978 for 1000 iterations which is pretty accurate and ideally this could go higher than the statistical model if allowed to converge, but this would take a really long time. <br />
The results can be seen in the plot MLP.png. <br />
REF: http://dstath.users.uth.gr/papers/IJRS2009_Stathakis.pdf

To run all files, execute the following command in root directory (course-2017-fal-proj):
```
python3 execute.py angelay_maulikjs
```
