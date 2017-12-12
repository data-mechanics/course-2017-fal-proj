var express = require('express');
var router = express.Router();
//mongostuff
var Db = require('mongodb').Db;
var Server = require('mongodb').Server;


//Get data from MongoDB
var schools = [];
var school_coords = [];

var hospitals = [];
var hospital_coords = [];

var policeDepts = [];
var policeDept_coords = [];

var streetlight_coords = [];

var neighborhood_scores = [];



var db = new Db('repo', new Server('localhost', 27017));
db.open(function(err, db) {

    if (err) { console.log ('Cannot connect to mongo, error message : ' + error); }

    db.collection('carole07_echanglc_wongi.schools_coord').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            //schools has [schoolName, coords]
            schools.push( [ result[i]['schoolName'] , result[i]['coord'] ]);
            //coords is just [ coords ]
            school_coords.push( result[i]['coord']);
        }
    });

    db.collection('carole07_echanglc_wongi.hospitals_coord').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            hospitals.push( [ result[i]['hospitalName'] , result[i]['coord'] ]);
            hospital_coords.push( result[i]['coord']);
        }
    });

    db.collection('carole07_echanglc_wongi.polices_coord').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            policeDepts.push( [ result[i]['policeDeptName'] , result[i]['coord'] ]);
            policeDept_coords.push( result[i]['coord']);
        }
    });

    db.collection('carole07_echanglc_wongi.streetlights_coord').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            streetlight_coords.push( result[i]['coord'] );
        }
    });

    db.collection('carole07_echanglc_wongi.neighborhood_scores').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            neighborhood_scores.push( [ result[i]['neighborhood'] , result[i]['score'] , result[i]['streetlights_count'] , result[i]['policeDept_count'] , result[i]['hospital_count'] , result[i]['school_count'] ]);
        }
    });

});


//console.log(neighborhood_scores.length);



//Render Index Page
router.get('/', function(req, res){
  res.render('index', {
    title: 'CS 591 carole07_echanglc_wongi',
    schools: JSON.stringify(schools),
    school_coords: JSON.stringify(school_coords),
    hospitals: JSON.stringify(hospitals),
    hospital_coords: JSON.stringify(hospital_coords),
    policeDepts: JSON.stringify(policeDepts),
    policeDept_coords: JSON.stringify(policeDept_coords),
    neighborhood_scores: JSON.stringify(neighborhood_scores),
    streetlight_coords: JSON.stringify(streetlight_coords)
  });
});

//Render About Page
router.get('/about', function(req, res){
  res.render('about', {
    title: 'About Page'
  });
});

module.exports = router;