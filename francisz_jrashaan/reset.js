/* ****************************************************************************
** 
** reset.js
**
** Script for setting up and initializing a Data Mechanics Repository instance
** within MongoDB.
**
** This script will reset the database so that it is ready for a fresh run of
** 'setup.js'; ALL data will be lost!
**
**   Web:     datamechanics.org
**   Version: 0.0.1
**
*/

// Load the configuration file.
var config = JSON.parse(cat("config.json"));

// Drop the repository database.
db = new Mongo().getDB(config.repo.name);
db.dropDatabase();

/* eof */