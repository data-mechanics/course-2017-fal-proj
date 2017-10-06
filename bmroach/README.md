# Datasets in Use
* Boston Local Climate Data (NOAA) [2008-2017]
* Hubway Trip Information (Hubway) [2011-2017]
* Boston Neighborhoods Yearly Rainfall (Boston Water and Sewer Commission) [1999-2017]
* Open Spaces (Analyze Boston)
* Existing Bike Network (Analyze Boston)


## Motivation behind selection of datasets
By analyzing climate, rainfall, bike-sharing, bike routes, and open space data, this 
collection of datasets aims to enable the gathering of insights into relationships between
Boston's neighborhoods and a novel outdoor-recreation quality of life metric (determining 
impact of outdoor-recreation on overall quality of life), an equation for which to be 
defined by those utilizing this collection. The team behind these algorithms imagines 
that the presence of more open spaces and bike routes, as well as an increased number 
of Hubway trips originating or terminating in a given neighborhood, may indicate a relatively
higher outdoor-recreation QOL figure. Additionally, by comparing climate data, as well as 
rainfall by neighborhood over time, additional variables regarding weather could factor 
into the outdoor-recreation QOL figure equation. While the Local Climate Data would not
offer the granularity needed for neighborhood-specific analysis, the inclusion of regional
climate data could validate the QOL metric for broader use. Moreover, the potential exists
to cross validate resulting per-neighborhood QOL figures with city government spending, 
available via Analyze Boston, aimed at expanding or improving outdoor spaces. 


### Python modules in use not typically included in standard Python distributions
* dml
* prov
* geojson

### Authentication for Datasets
* Hubway - __None__
* Local Climate Data - __None__
* Rainfall - __None__
* Existing Bike Network - __None__
* Open Space - __None__

### Authentication for Transformations
* Transform Hubway - Google Maps API
    * https://developers.google.com/maps/documentation/geocoding/get-api-key
    * auth.json should include a pair following the convention: 
        * { "GoogleMapsAPI": { "key": "YOUR-KEY" } }
    


### Three Output Datasets
* Aggregated Hubway Data by Neighborhood
* Desirable weather conditions (intended to serve as an example for later customization)
* 

### Resources
* Awesome tool to format curl commands as Python https://curl.trillworks.com
