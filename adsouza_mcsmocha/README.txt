Members:  
Adriana D'Souza
Monica Chiu

Narrative:
In order to find a way to improve the efficiency of garbage collection throughout Boston, we have compiled several datasets to locate problematic areas 
that showed inefficient garbage disposal trends. The datasets we have collected include: 311 Service Requests, Public Works Department (PWD), Crime Reports,
Big Belly Alerts, and CityScore. We selected, projected, and aggregated certain records from 311 Service Requests (our core dataset) that pertained to trash 
and used them in all three of our transformations because they contained the neighborhoods and the corresponding violations. We used these violations in 
addition to the corresponding districts (matched by transforming PWD dataset), related crimes (matched by transforming Crime Reports and using the districts 
from PWD), Big Belly alerts (matched by transforming Big Belly Alerts dataset and coordinates from 311 Service Requests), and CityScore to potentially find 
a solution that maximizes the rate of garbage collection. With this data, one can possibly map out areas for potential improvements to the waste management 
infrastructure.

Tools:
For exact list of packages used, look at req.txt within the same directory

Additional Notes:
As there are several datasets of considerable size (311 Service Requests especially) that we're working on and the transformations require parsing through 
them multiple times so to run the entire adsouza_mcsmocha folder to generate the results and provenance and as such would take a considerable amount of time.
Please keep this in mind when running the following files. You may have to comment out some sections when testing (namely within threeBigBellies.py).

Also note auth.json is not necessary as we do not require any authentication for our APIs and is simply a placeholder when running code.

