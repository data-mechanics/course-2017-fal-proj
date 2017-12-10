/**
 * Generates a random string containing numbers and letters
 * @param  {number} length The length of the string
 * @return {string} The generated string
 */

var express = require('express'); // Express web server framework
var app = express();
var request = require('request'); // "Request" library
var querystring = require('querystring');
var cookieParser = require('cookie-parser');
var body_parser = require('body-parser');
var MongoClient = require('mongodb').MongoClient;
const spawn = require('child_process').spawn;


app.use(body_parser.urlencoded({extended: false}));
app.use(body_parser({limit: '50mb'}));
app.use(body_parser.json());
var url = "mongodb://localhost:27017/biel_otis";
var apiKey = "AIzaSyBfjPoXnpYVcUh0ZnZmh_hIyKSmwicbKl8";
var mapsKey = "AIzaSyBDpdTtSILyhJFnFwTDm2RnPPPoH3W9j_8"
//https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=
/*
MongoClient.connect(url, function(err, db){
    db.collection("biel_otis.BostonZoning").find({}).toArray(function(err, res ){
        console.log(err);

    });

});
*/
app.get("/obesityData", function (req, res) {
    MongoClient.connect(url, function (err, db) {
        db.collection('biel_otis.ObesityData').find({"cityname": "Boston"}, {
            "geolocation.latitude": 1,
            "geolocation.longitude": 1
        }).toArray(function (error, result) {
            if (error == null) {
                res.send(JSON.stringify(result));
                return;
            }
            else {
                res.send(null);
                return;
            }
        });
    });

    //db.biel_otis.ObesityData.find({"cityname": "Boston"}, {"geolocation.latitude": 1, "geolocation.longitude": 1})


});


app.post("/newMeans", function (req, res) {
    console.log(JSON.stringify(req.body));

    //When we get an insert call we send store the data in the database
    console.log("We are inserting an item into the database");
    MongoClient.connect(url, function (err, db) {
        db.createCollection("biel_otis.UserObesityData", null);
        db.collection("biel_otis.UserObesityData").insertMany(req.body).then(function (response, error) {
        });
    });

        const pyProg = spawn('python3', ['./visualizationMeans.py']);
        //var pythonProg = execSync('python ./visualizationMeans.py');

        pyProg.on('exit', function(code, signal){
            console.log("In the exit function")
            MongoClient.connect(url, function (err, db) {
                console.log("here");
                db.collection("biel_otis.UserOptimalMarkets").find().toArray(function (error, result) {
                    if(result == null){
                        setTimeout(function(){
                            db.collection("biel_otis.UserOptimalMarkets").find().toArray(function(error,result){
                                res.send(JSON.stringify(result));
                            })
                        }, 5000)
                    }
                    res.send(JSON.stringify(result));
                });
            });
        });
});

app.post("/getAddressData", function(req, res){
    console.log(req.body);


});

app.use(express.static(__dirname + '/public'))
    .use(cookieParser());

module.exports = app;

