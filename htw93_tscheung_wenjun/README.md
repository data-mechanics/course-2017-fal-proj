# Project Report
## Members
**Haotian Wu**, **Desheng Zhang**, **Wenjun Shen**

## Project Narrative

As one of the most famous city all over the world,Boston is a popular city for people to travel. People might use website like booking or tripdavisor to find out the suitable hotel from them. However, most of the grades and comments concentrates on the hotels themselve, like the quality of service, cleaningness, etc. Obviously, those grades and comments ignore the surroundings, like crime rates, transportations. Hence we try to evaluate the hotels of Boston by some other factors, like crime rate, transportation information, foods and gardens.


## Datasets

In this project, we use 5 original datasets. Our core dataset is the hotels of Bostons, which we scraped using BeautifulSoup with Python. Besides that, we have crime data, MBTA data, restaurant data and garden data of Boston. 

1. [**Hotels in Boston**](http://datamechanics.io/data/htw93_tscheung_wenjun/Hotel_ratings.json)
2. [**Boston Crime Data**](https://data.cityofboston.gov/resource/29yf-ye7n.json) 
3. [**MBTA Data**](http://datamechanics.io/data/htw93_tscheung_wenjun/MBTA_Stops.txt)
4. [**Restaurants in Boston**](https://data.cityofboston.gov/resource/fdxy-gydq.json)
5. [**Gardens of Boston**](https://data.cityofboston.gov/resource/rdqf-ter7.json)

## Data Transformation

Orginal Dataset | Transformation Description| New Dataset
---- | ---| ---
Hotels in Boston, Boston Crime Data, MBTA Data,Restaurants in Boston, Gardens of Boston | Start from each hotel, set a radius(ex. 0.5 miles), count number of crimes, mbta stops, restaurants and gardens | Boston Hotel Data
BostonHotelData |  Apply our custom algorithm to give a new score to each hotel | BostonHotelCustomScore
BostonHotelCustomScore| Apply Correlation coefficient to figure out the most related factor to our custom score system | BostonHotelCorrelation



## Custom Rating System

In our new scoring system, we calculate the number of gardens, crimes, MBTA stops, restaurants and cafes near each hotels(within certain distance). Then we use the normalize formula below to scale the original sorce and datas gathered together to calculater the new score.

### Normalize Formula
<img src="https://i.imgur.com/HC093vp.png" style="width:200px">

### Custom Score Formula
<img src="https://i.imgur.com/PQ6ekYB.png" style="width:400px">

## Correlation Coefficient
**Formula:**

<img src="https://i.imgur.com/YipOLbT.png" style="width:200px">

For each factor, we calculate the correlation coefficient. below is the result:

### Crime
* coefficient: -0.09448652255976357
* p value: 0.3984638884028119

### MBTA Stops
* coefficient: 0.5511472545817303
* p value: 8.065664653031119e-8

### Gardens
* coefficient: 0.8712154440427992
* p value: 1.9353053027735632e-26

### Foods
* coefficient: 0.6383325374109514
* p value: 1.1093179778057194e-10

## Best place to build a hotel

Based on the effort we made above, we try to find a best place to build to hotel. In the part, we first discard useless factors and recalculate the custom score of each hotel. Then we use the coordinates of the hotel and the new score to do K-means. We choose the cluster with highest average score, the calcuate the center coordinate of this cluster, which will be the best place to build a new hotel.

### Factor Filter

Learnt from the correlation coefficient, we find out that our custom score is nearly not related to crime. Thus we discard crime factor first.

### New Custom Score

We discard the crime factor, thus the new score will be:

<img src="https://i.imgur.com/1A9lMiu.png" style="width:300px">

### Apply K-means
For K-means matrix, we use the coordinates and new custom rates of hotels. First we normalize the coordinates and rates separately. Then we slightly make coordinates with higer weights to make sure the clusters can be clusted based on there location first. We choose number of clusters as 10. For each calculated cluster. We select the cluster with higheset average custom rate and calculate the center coordinate of this cluster, which is [42.347708499999996 -71.0792716]

