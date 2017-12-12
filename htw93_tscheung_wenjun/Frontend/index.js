/**
 * Created by haotianwu on 12/2/17.
 */

// Global variables
var map;
var geocoder;
var finalOrder = ["0","1","2","3"];
var markers = [];
var numOfResults;
var prev_infoWindow =false;
var choiceList = document.getElementById('choiceList');

$(document).ready(function() {
    $('select').material_select();
});

new Sortable(choiceList,{
    group: "words",
    store: {
        get: function (sortable) {
            var order = localStorage.getItem(sortable.options.group);
            return order ? order.split('|') : [];
        },
        set: function (sortable) {
            var order = sortable.toArray();
            finalOrder = order
            console.log(order.toString())
            //localStorage.setItem(sortable.options.group, order.join('|'));
        }
    }
});

/*
 initialize the map.
 */
function initMap() {
    var astorPlace = {lat: 37.783, lng: -122.417}                   //set original place to San Francisco

    map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: 42.351571, lng: -71.080008},
        zoom: 14,
        streetViewControl: false,
        styles: [{
            "featureType": "landscape",
            "stylers": [{"hue": "#FFBB00"},
                {"saturation": 43.400000000000006},
                {"lightness": 37.599999999999994}, {"gamma": 1}]
        },
            {
                "featureType": "road.highway", "stylers": [{"hue": "#FFC200"},
                {"saturation": -61.8}, {"lightness": 45.599999999999994},
                {"gamma": 1}]
            },
            {
                "featureType": "road.arterial",
                "stylers": [{"hue": "#FF0300"},
                    {"saturation": -100},
                    {"lightness": 51.19999999999999}, {"gamma": 1}]
            },
            {
                "featureType": "road.local",
                "stylers": [{"hue": "#FF0300"},
                    {"saturation": -100},
                    {"lightness": 52}, {"gamma": 1}]
            },
            {
                "featureType": "water",
                "stylers": [{"hue": "#0078FF"},
                    {"saturation": -13.200000000000003},
                    {"lightness": 2.4000000000000057}, {"gamma": 1}]
            },
            {
                "featureType": "poi",
                "stylers": [{"hue": "#00FF6A"},
                    {"saturation": -1.0989010989011234},
                    {"lightness": 11.200000000000017}, {"gamma": 1}]
            }]
    });

    geocoder = new google.maps.Geocoder();
    panorama = map.getStreetView();
    panorama.setPosition(astorPlace);
}


function sendData() {
    clearMarkers();
    console.log("Send data")
    console.log(finalOrder)
    numOfResults = getNum(document.querySelector('input[name="numResult"]:checked').id);
    console.log(numOfResults);
    $.ajax({
        url : 'http://155.41.118.36:8080/request/',
        type: 'POST',
        data:JSON.stringify({data:finalOrder}),
        success : handleData
    });
}

function handleData(data) {
    var hotels = data["cluster"];
    for (i = 0; i < numOfResults; i++) {
        if (i == hotels.length) {
            if (numOfResults != 100)
                Materialize.toast('Results is less than ' + numOfResults, 3000, 'red');

            break;
        }
        var myLat = parseFloat(hotels[i]["lat"]);
        var myLong = parseFloat(hotels[i]["long"]);
        var score = parseFloat(hotels[i]["score"]);
        var pos = {lat: myLat, lng: myLong};
        var hotelName = hotels[i]['hotel'];
        createMarker(hotelName, pos, score, i + 1, map);
    }
}

function createMarker(name, pos, score, rank, map) {
    var marker = new google.maps.Marker({
        position: pos,
        map: map,
        title: name,
        icon: 'http://chart.apis.google.com/chart?chst=d_map_pin_letter&chld=' + rank + '|FE6256|000000'
    });

    var infoWindow = new google.maps.InfoWindow({
        content: "<p><b>" + name + "</b></p>" //+
                    //"<p><b>Rating: " + score + "</b></p>"
    });

    marker.addListener('click', function() {
        if( prev_infoWindow ) {
            prev_infoWindow.close();
        }

        prev_infoWindow = infoWindow;
        infoWindow.open(map, marker);
    });

    markers.push(marker);
}

/*
 clean markers.
 */
function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
        markers[i].setMap(null);

    }
    markers = [];
}

function getNum(id) {
    if (id == "num5") {
        return 5;
    }
    if (id == "num10") {
        return 10;
    }
    if (id == "num15") {
        return 15;
    }
    if (id == "numAll") {
        return 100;
    }
}

