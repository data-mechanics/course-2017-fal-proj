## CS591 Project-1

# Justification 
We have included the following datasets:

**(Snow Emergencies)**
http://bostonopendata-boston.opendata.arcgis.com/datasets/4f3e4492e36f4907bcd307b131afe4a5_0

**(Traffic Signals**) http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0

**(Buildings)** https://data.boston.gov/dataset/buildings

**(Census (Block Groups) 2010)** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/5AF9LK

**Census (Blocks) 2010)** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/FI1YED

**(Roads)** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/OV5PXF

**(Boston Neighborhoods)** http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0

We believe that these datasets will allow us to answer the following question: "How can we create snow emergency routes that will pollute traffic in the city of Boston the least?" Since we have all the building blocks (no pun intended) for displaying city infrastructure with our datasets (buildings, roads), we are able to roughly map out how the roads are interconnected and shaped. Next, we have the census that will roughly (because they are slightly outdated due to data being formed at 2010) know how many ...(line truncated)...

# Pulling CSVs
Two of our datasets are in the form of CSVs, specifically our census and roads data. We uploaded these to datamechanics.io and pulled them in their respective get modules. These are pulled as strings, we parse them line by line to turn into dictionaries and insert into Mongo.
