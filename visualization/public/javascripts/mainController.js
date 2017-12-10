/**
 * Created by jotis on 6/8/2017.
 */


/**
 * Created by jotis on 6/8/2017.
 */

var app = angular.module('main', []); //Application is now bound to landing
var baseUrl = 'http://localhost:8080';   //Should be modified in production
app.config(["$sceDelegateProvider", function ($sceDelegateProvider) {
    $sceDelegateProvider.resourceUrlWhitelist([
        // Allow same origin resource loads.
        "self",
        // can add other dependencies here for access to external resources.
    ]);
}]);

app.controller('mainController', function ($scope, $element, $timeout, $http, $document, $window) {
    //"use strict";
    $scope.markerArray = [];
    $scope.meansArray = [];

    var mymap = L.map('mapid').setView([42.350484, -71.069760], 13);
    L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
        maxZoom: 18,
        id: 'mapbox.streets',
        accessToken: 'pk.eyJ1IjoiamFkb3RpcyIsImEiOiJjamF6dXBhODk2dWc1MzFucXBtaDRkM3AwIn0.YyCWTkvdskHlrpSHZgk9Ew'
    }).addTo(mymap);

    var redDot = L.icon({
        iconUrl: '../images/Red_dot.svg',
        iconSize:     [15, 15], // size of the icon
    });
    var blueDot = L.icon({
        iconUrl: '../images/Bluedot.svg',
        iconSize:     [20, 20], // size of the icon
    });

    $http({
        url: baseUrl + '/obesityData',
        method: 'GET',
        headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' }
    }).then(function (success) {

        for(var individual in success.data) {
            var marker = new L.marker([success.data[individual]["geolocation"]["latitude"], success.data[individual]["geolocation"]["longitude"]], {
                draggable: true,
                icon: redDot
            }).addTo(mymap);
            $scope.markerArray.push(marker);
        }
    }, function (error) {
        console.log(error);
    });

    $scope.calcKMeans = function(){
        var points = []
        for(var x in $scope.markerArray){
            points.push($scope.markerArray[x]["_latlng"])
        }

        if($scope.markerArray == []){
            $scope.errorText = "there are no points to plot!";
        }
        else{
            $scope.loader = true;
            $http({
                url: baseUrl + '/newMeans',
                method: 'Post',
                data: points,
                headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' }
            }).then(function (success) {
                debugger;
                if($scope.meansArray.length > 0 ){
                    for(var x in $scope.meansArray){
                        mymap.removeLayer($scope.meansArray[x]);
                    }
                    $scope.meansArray = [];
                }
                for(var individual in success.data[0]["optimalMarketLocation"]) {
                    var marker = new L.marker([success.data[0]["optimalMarketLocation"][individual][0], success.data[0]["optimalMarketLocation"][individual][1]], {
                        draggable: false,
                        icon: blueDot
                    }).addTo(mymap);
                    $scope.meansArray.push(marker);
                }
                $scope.loader = false;
            }, function (error) {
                console.log(error);
            });
        }
    }

    $scope.plot = function () {
        console.log($scope.address)
        if($scope.address == null || $scope.address == ""){
            $scope.errorText = "Please enter an address for a calculation...";
            $scope.err =  true;
            return;
        }
        else{
            $scope.loader = true;
            $scope.errorText = "";
            $scope.err =  false;
            $http({
                url: baseUrl + '/getAddressData',
                method: 'Post',
                data: [$scope.address],
                headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' }
            }).then(function (success) {
                $scope.loader = false;

                //success
            }, function (error) {
                //Error
            });

        }

    } //End plot

});
