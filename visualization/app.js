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
    var query = req.body[0].replace(new RegExp(' ','g'), '+');
    var url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + query + '&' + mapsKey;
    console.log(url);
    request.get(url, function (error, response, body) {
        console.log(body);
        body = JSON.parse(body);
        var zip = "";
        var addresses = body["results"][0]["address_components"];//[7]["long_name"];
        for(var name in addresses){
            if(addresses[name][0].hasAttribute("types") && addresses[name][0]["types"] == "postal_code"){
                zip = addresses[name]["long_name"];
            }
        }
        if(zip == ""){
            res.send("");
            return;
        }



        var lat = body["results"][0]["geometry"]["bounds"]["northeast"]["lat"];
        var lng = body["results"][0]["geometry"]["bounds"]["northeast"]["lng"];
        console.log("lat: " + lat + "\n" + "lng: " + lng);
        res.send("");
    });

});

app.use(express.static(__dirname + '/public'))
    .use(cookieParser());

module.exports = app;

