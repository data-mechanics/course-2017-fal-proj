# course-2017-fal-proj
Joint repository for the collection of student course projects in the Fall 2017 iteration of the Data Mechanics course at Boston University.

In this project, you will implement platform components that can obtain a some data sets from web services of your choice, and platform components that combine these data sets into at least two additional derived data sets. These components will interct with the backend repository by inserting and retrieving data sets as necessary. They will also satisfy a standard interface by supporting specified capabilities (such as generation of dependency information and provenance records).

**This project description will be updated as we continue work on the infrastructure.**

## Datasets
**Bike Network Data from Boston Maps Open Data**

**Neighborhood Data from Analyze Boston**

**Hubway Station Data from Analyze Boston**

**Charging Station Data from Boston Maps Open Data**

**Open_Space.csv from BostonMaps: Open Data;**


## Narrative
Expanding public transportation and car sharing are two of the most popular solutions to reduce emissions in urban environments. However, these options are not necessarily zero emission options and may still pollute the environment. Therefore it's important that more effective measures be considered to reduce emissions.

By looking at charging stations, hubway stations, biking networks, and open space data in each Boston neighborhood we determined a green score for each neighborhood. From here we created a statistical analysis in an attempt to find out if there exists any correlation between subset entities of our data and if these correlations corresponded to the number of placements of select entities in each neighborhood. To do this, we iterated through all the possible subsets of two entities within neighborhoods and calculated correlations.

Finally we took the green scores we computed initially and set up a constraint satisfaction problem where we attempted to optimize the green score for each neighborhood given a budget of $1,000,000. The constraints we added attempted to make the computation realistic, meaning that solutions of 0 or less than 0 were not acceptable, in an effort to deplete the budget.We also randomized the maximum and minimum number of specific entities that could be built in neighborhoods in an effort to create a unique solution for each neighborhood.

Moreover, we have made a web visualization which uses flask to retrieve the data from our mongo db collections, and presents the data using leaflet js , and an interactive slider made using D3.js which invokes our modified constraint satisfaction algorithm which we tweaked to include a higher range of budgets and more constraints. The interactive visualization lets the user pick through a range of budgets and watch the number of green facilities/services change as well as the score with each budget. 

There is a supplemental report and presentation as well. 



## Setting Up
Please run neighborhoodscores.py, followed by budgets.py, and then run app.py ** it must be in this order to work. 
The web visualization will be made available at localhost:3000



Notes:
Our Z3 files are modified slightly:
For Z3core, Z3printer and Z3 we removed the ". import" lines
If you do not have z3 on your machine we have added a supplementary folder which contains z3, all that needs to be done is to move the files inside the folder to the parent file titled francis_jrashaan. Additionally, we set the trial parameter to true.




## MongoDB infrastructure

### Setting up

We have committed setup scripts for a MongoDB database that will set up the database and collection management functions that ensure users sharing the project data repository can read everyone's collections but can only write to their own collections. Once you have installed your MongoDB instance, you can prepare it by first starting `mongod` _without authentication_:
```
mongod --dbpath "<your_db_path>"
```
If you're setting up after previously running `setup.js`, you may want to reset (i.e., delete) the repository as follows.
```
mongo reset.js
```
Next, make sure your user directories (e.g., `alice_bob` if Alice and Bob are working together on a team) are present in the same location as the `setup.js` script, open a separate terminal window, and run the script:
```
mongo setup.js
```
Your MongoDB instance should now be ready. Stop `mongod` and restart it, enabling authentication with the `--auth` option:
```
mongod --auth --dbpath "<your_db_path>"
```

### Working on data sets with authentication

With authentication enabled, you can start `mongo` on the repository (called `repo` by default) with your user credentials:
```
mongo repo -u alice_bob -p alice_bob --authenticationDatabase "repo"
```
However, you should be unable to create new collections using `db.createCollection()` in the default `repo` database created for this project:
```
> db.createCollection("EXAMPLE");
{
"ok" : 0,
"errmsg" : "not authorized on repo to execute command { create: \"EXAMPLE\" }",
"code" : 13
}
```
Instead, load the server-side functions so that you can use the customized `createCollection()` function, which creates a collection that can be read by everyone but written only by you:
```
> db.loadServerScripts();
> var EXAMPLE = createCollection("EXAMPLE");
```
Notice that this function also prefixes the user name to the name of the collection (unless the prefix is already present in the name supplied to the function).
```
> EXAMPLE
alice_bob.EXAMPLE
> db.alice_bob.EXAMPLE.insert({value:123})
WriteResult({ "nInserted" : 1 })
> db.alice_bob.EXAMPLE.find()
{ "_id" : ObjectId("56b7adef3503ebd45080bd87"), "value" : 123 }
```
If you do not want to run `db.loadServerScripts()` every time you open a new terminal, you can use a `.mongorc.js` file in your home directory to store any commands or calls you want issued whenever you run `mongo`.

## Other required libraries and tools

You will need the latest versions of the PROV, DML, and Protoql Python libraries. If you have `pip` installed, the following should install the latest versions automatically:
```
pip install prov --upgrade --no-cache-dir
pip install dml --upgrade --no-cache-dir
pip install protoql --upgrade --no-cache-dir
```
If you are having trouble installing `lxml` in a Windows environment, you could try retrieving it [here](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

Note that you may need to use `python -m pip install <library>` to avoid issues if you have multiple versions of `pip` and Python on your system.

## Formatting the `auth.json` file

The `auth.json` file should remain empty and should not be submitted. When you are running your algorithms, you should use the file to store your credentials for any third-party data resources, APIs, services, or repositories that you use. An example of the contents you might store in your `auth.json` file is as follows:
```
{
"services": {
"cityofbostondataportal": {
"service": "https://data.cityofboston.gov/",
"username": "alice_bob@example.org",
"token": "XxXXXXxXxXxXxxXXXXxxXxXxX",
"key": "xxXxXXXXXXxxXXXxXXXXXXxxXxxxxXXxXxxX"
},
"mbtadeveloperportal": {
"service": "http://realtime.mbta.com/",
"username": "alice_bob",
"token": "XxXX-XXxxXXxXxXXxXxX_x",
"key": "XxXX-XXxxXXxXxXXxXxx_x"
}
}
}
```
To access the contents of the `auth.json` file after you have loaded the `dml` library, use `dml.auth`.

## Running the execution script for a contributed project.

To execute all the algorithms for a particular contributor (e.g., `alice_bob`) in an order that respects their explicitly specified data flow dependencies, you can run the following from the root directory:
```
python execute.py alice_bob
```
To execute the algorithms for a particular contributor in trial mode, use the `-t` or `--trial` option:
```
python execute.py alice_bob --trial
```
