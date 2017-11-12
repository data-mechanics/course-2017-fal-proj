# Optimal Placement of Speed Feedback Signs in the City of Boston
<img src='https://www.boston.gov/sites/default/files/speed-limit-3.jpg' height='200' width='auto'><br>
*source: City of Boston*


## The Problem
#### Introduction and Motivation
Our team set out to determine an optimal placement of Speed Feedback Signs in the City of Boston. To do this, we analyzed where accident hotspots were around the city (further referred to as clusters), and where vulnerable areas are. Despite a plethora of types of potentially vulnerable areas, we decided to focus on schools, hospitals, and open spaces, such as parks. We used these locations collectively as 'triggers', or sites of equal weight in our scoring algorithm. This problem was previously hosted as an [open data challenge](https://docs.google.com/document/d/11QtIfhwWJEDumRgzKkkH68bzh9qrra15vVwvuNsz_oY/mobilebasi), but we sought to give it an additional look
#### Explanation of Process
We categorized our approach into two parts. <br>
**Part 1** - Placement of Speed Feedback Signs
* Phase 1:<br>
Cluster accidents into accident hot spots via k-means, where the number of means is proportional to the number of input nodes.
* Phase 2: <br>
Filter intersections by proximity to accident clusters. For an intersection to be a candidate placement site, the intersection must be in the lower 50th percentile with regards to distance to closest accident cluster. This ensures that final placements are not skewed by proximity to vulnerable sights alone, but must also be close to where accidents are known to occur. 
* Phase 3:<br>
We consider school, hospital, and open space locations, as well as accident cluster locations, as equally weighted 'triggers', or data points. We then run k-means on this data set, with k = 30 (hard-coded, imagining 30 of these signs are available). We then find the candidate intersection closest to each of these determined means (output of k-means), and output these 30 intersections as the sites of the speed feedback sign placements.
<br>

**Part 2** - Statistical Analysis
* (analysis 1 - tbd)
* (analysis 2 - tbd)
* (analysis 3 - tbd)

#### Technical Details
* Each unique portion of our process is its own extension of the dml library's algorithm class, and intermediate data is stored using MongoDB.
* --trial flag for execute.py is being leveraged as a verbose flag.

## Statistical Findings
...tbd... 

## Datasets in Use
* Motor Vehicle Accidents (Analyze Boston)
* Hospital Locations (Analyze Boston)
* Street Intersections (Boston Open Data - opendata.arcgis.com)
* Open Spaces (Boston Open Data - opendata.arcgis.com)
* Schools (boston.opendatasoft.com)

## Scripts
* *fetch_accidents.py*
* *fetch_hospitals.py*
* *fetch_nodes.py*
* *fetch_open_space.py*
* *fetch_schools.py*
* *fetch_street_info.py*
<br><br>
* *get_accident_clusters.py* - Performs k-means on the input accidents to reduce accidents into accident clusters, which are later used as points of influence as to where feedback signs should be placed.
* *get_signal_placements.py* - Consumes the triggers produced by clean_triggers (below) to determine the optimal placement of speed 
<br><br>
* *clean_triggers.py* - Collects and cleans accident clusters, schools, open spaces, hospitals, candidate intersections for placement for use as points in the k-means clustering done in get_signal_placements.
<br><br>
* *make_graph.py* - Plots the determined locations for speed feedback sign placements.


## Notes
* No Authentication for Datasets
* No Authentication for Transformations
* The resource libspacialindex is required to run this set of scripts. On macOS, it can be installed with Homebrew: brew install spatialindex. 

### Python modules in use not typically included in standard Python distributions 
* dml
* geojson
* geoql
* numpy
* pandas
* prov
* scipy
* sklearn
* To easily get the environment used, run the following command with the provided environment.yml file (make sure in team folder): conda env create -f environment.yml


### Team Members:
* Adriana D'Souza .......... adsouza@bu.edu
* Brian Roach ................. bmroach@bu.edu
* Jessica McAloon ......... mcaloonj@bu.edu
* Monica Chiu ................ mcsmocha@bu.edu