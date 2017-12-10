## Justification
Boston is a city notorious for extreme winter weather. With snowstorms potentially causing problems ranging from minor inconveniences like traffic backup to more serious concerns like roads to hospitals being blocked, it's important to clear snow from roads as efficiently as possible. To assist in efficient snow removal efforts, we classify the effectiveness of plowing a particular road in two ways: "road priority" and "connections". Road priority gauges how much plowing a particular road would benefit traversal throughout the city. At this stage of the project we are gauging this based on emergency snow routes. We will define a "high priority set" as a subset of Boston's emergency snow routes in which all other streets have access to at least one emergency route. We optimize this by finding the high priority set with the fewest emergency routes possible. "Connections" gauges how plowing a particular road would benefit access to nearby buildings of importance, and is optimized via a run of the K-means algorithm, wherein clusters to be closed in on by K-means are defined by the coordinates of these buildings of importance. We then perform analysis of the K-means result to return the distance to the nearest centroid for each point of importance.
In our visualization, the K-means result is displayed alongside the constraint satisfaction result. Roads to be prioritized by the city to plow first, are those with the smallest distance between them, and the nearest centroid.

## Requirements
* Our code was designed and tested to run with Python 3.6
* The following libraries must be installed to run execute.py:
    * numpy
    * scipy
    * sklearn
    * z3-solver

To install the z3 library properly please visit the github page and follow the instructions for python installation.

github link:

https://github.com/Z3Prover/z3

## Notes and Running
* Within the repository there is a folder entitled "extra_files". The files in this directory do not, and should not, run upon execution. They are files which are not used in the context of the current project, but may prove useful in project 3.
* A Google Maps API key is required. One call will be made to generate a map. If generation of markers is enabled, one API call per marker will also be made to Google Maps' geocoordinate API, allowing us to find the geographic center of each route selected by the constraint satisfaction problem. See below for more details on the generation of markers. This key is pulled from an auth.json file into server.py using the indexes ```['services']['googleportal']['key']```. Google Maps' free API keys allow for 2,500 calls per day.

* To run:

```
mongod --dbpath "<DATABASE PATH HERE>"
```

```
mongo repo -u bkin18_cjoe_klovett_sbrz -p bkin18_cjoe_klovett_sbrz --authenticationDatabase repo
```

* In the base directory "course-2017-fal-proj",
For trial mode:
```
python3 execute.py bkin18_cjoe_klovett_sbrz --trial
```
Else:
```
python3 execute.py bkin18_cjoe_klovett_sbrz
```

* cd into the "visualization/" directory.
'''
python server.py
'''

Enter in a number of means to be used as a value for k in the k-means algorithm. Our implementation allows for 1-21 means.
Enter in a number of routes to be used as the number of routes returned by the z3-solver.
Toggle whether markers should be generated. If enabled, the map will take roughly 0.5 to 2.0 seconds per marker to generate, with a marker existing for each route returned by the z-3 solver. This runtime increase appears to be due to limitations on the standard usage limits of Google Maps' geocoder API, which allows for a limited number of queries per second. For more information, [see here](https://developers.google.com/maps/documentation/geocoding/usage-limits). To monitor the current progress, check the terminal window running python server.py once "submit" has been clicked. In some cases the server might time out in the generation of markers, especially for high number of routes.
Click "submit."

# course-2017-fal-proj
Joint repository for the collection of student course projects in the Fall 2017 iteration of the Data Mechanics course at Boston University.

In this project, you will implement platform components that can obtain a some data sets from web services of your choice, and platform components that combine these data sets into at least two additional derived data sets. These components will interct with the backend repository by inserting and retrieving data sets as necessary. They will also satisfy a standard interface by supporting specified capabilities (such as generation of dependency information and provenance records).

**This project description will be updated as we continue work on the infrastructure.**

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
