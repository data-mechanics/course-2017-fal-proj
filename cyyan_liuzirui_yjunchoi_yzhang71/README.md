# The Purpose of Project
As an every presidential election, every election is the most important election in our lifetime before knowing the result of election, because we can change the result by voting. However, based on the research done by our group, more than 40% of U.S citizen did not vote in 2016. With researching the relationship between voter turnout and the other factors, we find the journal: "Increasing Voter Turnout: Can Mass Transit Help?" (http://www.inquiriesjournal.com/articles/1618/increasing-voter-turnout-can-mass-transit-help). By using dataset from various data sources of Boston, we focus on optimizing the polling locations based on public transportation. Moreover, we compare between the original polling location and the optimized polling location by calculating the average distances with randomized samples of voters in Boston (95% confidence interval).

## Authors
Yueyan Chen (cyyan)
Zirui Liu (liuzirui)
Young Jun Choi (yjunchoi)
Yuchen Zhang (yzhang71)

## Data set
###Data set for Wards:
https://data.boston.gov/dataset/wards
###Data set for Polling Location:
https://data.boston.gov/dataset/polling-locations
###Data set for Bus Stop:
http://datamechanics.io/data/wuhaoyu_yiran123/MBTA_Bus_Stops.geojson
###Data set for MBTA:
http://erikdemaine.org/maps/mbta/mbta.yaml
###Data set for President Election Result by Precinct:
http://datamechanics.io/data/yjunchoi_yzhang71/presidentElectionByPrecinct.csv

## Optimization and Statistical Analysis
### K-Means (optByPublicT.py; optByBusstop.py; optByMBTA.py)
We use K-means algorithms to find the optimal polling locations based on public transportations, bus stops, and MBTA. In each file, we divide bus stops and MBTA stations into 22 wards, and use K-means algorithms 22 times to find the optimal polling locations in each ward. For optByPublicT.py, we compute optimization with both bus stops and MBTA to compare the results among the other optimization methods. Therefore, three files return different lists of optimal polling location in each ward.

### Statistical Analysis with Sampling and Inference (scoringLocation.py)
Because it is difficult to tell which optimization method is the best without scoring or evaluating locations, we perform the basic statistical analysis with four different lists of polling locations. We randomize 10000 addresses in Boston for voters' addresses, instead of using every voter's address in Boston to compare among polling locations we optimized. By calculating Euclidean distance between randomized voter's address and the nearest polling location, we want to find which optimization method provides the highest accessibility to Boston voters. Throughout the distribution of distance between voters and the polling location, scoringLocation.py returns the result of statistical analysis in 95% confidence interval.

## Required libraries and tools
You will need some libraries and packages. By downloading through pip, you can easily install the latest versions.
```
python -m pip install yaml
python -m pip install numpy
python -m pip install pandas
python -m pip install matplotlib
python -m pip install math
python -m pip install scipy
python -m pip install csv
```
## To run the execution script for Project 2

```
python execute.py cyyan_liuzirui_yjunchoi_yzhang71
```
To execute the algorithms in trial mode
```
python execute.py cyyan_liuzirui_yjunchoi_yzhang71
```
