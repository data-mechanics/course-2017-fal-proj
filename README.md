# Keith Lovett Project One

##Summary
The goal of my project is to create an optimal placement locations for Big Bellies in the city of Cambridge. Placement will be based on the varying efficiency of the Big Bellies that are already found around the city of Boston. I will use data concerning the frequency of Big Belly "fullness" and measure how their surroundings effect this frequency in order to develop a metric for gauging their efficiency. Big Bellies will then be placed using this metric in the city of Cambridge. This will hopefully allow for greater waste management in the area.

##Setting Up
No major set-up is required. Inside the directory, running
```
mongod -auth -dbpath "<DATABASE PATH HERE>"
```

```
mongo repo -u klovett -p klovett --authentication Database "repo"
```

```
python3 execute.py klovett
```

should obtain all collections, perform all transformations, and create the appropriate provenance.