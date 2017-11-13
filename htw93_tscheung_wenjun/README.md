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
Boston Hotel Data |  Apply our custom algorithm to give a new score to each hotel | 


## Algorithm

### Scoring Formula


### Correlation Coefficient




