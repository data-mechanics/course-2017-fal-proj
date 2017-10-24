# angelay_Project1
In this project, I performed preparations for modeling CO2 emissions by country. I plan to use data from one year to build the model and test it on data from another year.

I first downloaded data on CO2 emissions (MMTons) from the Energy Information Administration:
https://www.eia.gov/beta/international/data/browser/#/?pa=00000000000000000000000002&c=ruvvvvvfvtvnvv1urvvvvfvvvvvvfvvvou20evvvvvvvvvnvvuvo&ct=0&vs=INTL.44-8-AFG-MMTCD.A&vo=0&v=H&end=2014

Then I downloaded data on GDP per capita (2011 PPP $) and Human Development Index from the United Nations Development Programme:
http://hdr.undp.org/en/data

Finally, I downloaded data on carbon intensity (kg per kg of oil equivalent energy use), energy intensity (MJ/$2011 PPP), energy use (kg of oil equivalent per capita) and total populations from The World Bank:
https://data.worldbank.org/

I manually cleaned up all the data because it would be difficult to perform cleanup in python since there are different ways to write some country names. I chose data from 2012 and 2013 because they were fairly comprehensive and recent. I organized the data and stored them as .json files, and then uploaded them to http://datamechanics.io/ in order to access them.

getData.py retrieves the 7 datasets <br />
merge2012.py merges all the data from 2012 and puts them into a collection called all2012. There are 129 entries in all2012. <br />
removeOutliers.py takes the data in all2012 and removes all entries with outliers. This leaves us 87 entries left and stored in collection clean2012. <br />
correlations.py takes all the data from clean2012 and returns the correlation coefficient of CO2 emissions and all 6 other attributes: carbon intensity, energy intensity, energy use, GDP per capita, Human Development Index, and population. The results are stored in collection corr2012. <br />

To run all files, execute the following command in root directory (course-2017-fal-proj):
```
python3 execute.py angelay
```