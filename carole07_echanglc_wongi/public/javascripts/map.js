var map; //important

google.maps.event.addDomListener(window, 'load', initialize); //load map


//get data from mongoDB
var schools = JSON.parse(document.getElementById('schools').innerHTML);
var school_coordsFromHTML = JSON.parse(document.getElementById('school_coords').innerHTML);
var school_coords = generateGooglePoints(school_coordsFromHTML);

var hospitals = JSON.parse(document.getElementById('hospitals').innerHTML);
var hospital_coordsFromHTML = JSON.parse(document.getElementById('hospital_coords').innerHTML);
var hospital_coords = generateGooglePoints(hospital_coordsFromHTML);

//Crime takes some time to load
var policeDepts = JSON.parse(document.getElementById('policeDepts').innerHTML);
var policeDept_coordsFromHTML = JSON.parse(document.getElementById('policeDept_coords').innerHTML);
var policeDept_coords = generateGooglePoints(policeDept_coordsFromHTML);

//property also takes some time to load
var streetlight_coordsFromHTML = JSON.parse(document.getElementById('streetlight_coords').innerHTML);
var streetlight_coords = generateGooglePoints(streetlight_coordsFromHTML);

//using coordinates, we generate google (longitude,latitude) for each point
function generateGooglePoints(category) {
    googArray = [];
    for (var i = 0; i < category.length; i++ ) {
        googArray.push( new google.maps.LatLng( category[i][0] , category[i][1] ) );
    }
    return googArray;
}

function initialize() {

    //initialize map
    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 12,
        center: { lat: 42.342132, lng: -71.103023 }, //Boston
    });

    //put the legend on the bottom left corner of the map
    map.controls[google.maps.ControlPosition.LEFT_BOTTOM].push(document.getElementById('legend'));

    //Create a marker on the map
    google.maps.event.addListener(map, 'click', function(marker) {
        addMarker(marker.latLng); //add marker to map
        getClosest(marker.latLng,hospital_coords,'hospitals'); //find nearest hospital
        getClosest(marker.latLng,school_coords,'schools'); //find nearest school
        getClosest(marker.latLng,policeDept_coords,'policeDepts'); //find nearest policeDept 
        calculateScore(marker.latLng);
    });

    //set the default text
    document.getElementById('origin_address').innerHTML = 'Marker Address: ';
    document.getElementById('score').innerHTML = 'Score: ';

    document.getElementById('hospital_name').innerHTML = 'Hospital Name: ';
    document.getElementById('dest_address_hospital').innerHTML = 'Closest Hospital Address: ';
    document.getElementById('distance_hospital').innerHTML = 'Distance to Hospital: ';

    document.getElementById('school_name').innerHTML = 'School Name: ';
    document.getElementById('dest_address_school').innerHTML = 'Closest School Address: ';
    document.getElementById('distance_school').innerHTML = 'Distance to School: ';

    document.getElementById('policeDept_name').innerHTML = 'Police Department Name: ';
    document.getElementById('dest_address_policeDept').innerHTML = 'Closest Police Department Address: ';
    document.getElementById('distance_policeDept').innerHTML = 'Distance to Police Department: ';

}

////////////////////////////
// Heatmaps and Toggles
////////////////////////////

//init school heatmap
var heatMapSchool = new google.maps.visualization.HeatmapLayer({
    data: school_coords,
    map: map,
    radius: 40
});

//Toggle school heatmap
function toggle_heatmap_school() {
    heatMapSchool.setMap(heatMapSchool.getMap() ? null : map);
}


//init hospital heatmap toggle
var heatMapHospital = new google.maps.visualization.HeatmapLayer({
    data: hospital_coords,
    map: map,
    radius: 40,
    gradient: [
          'rgba(0, 255, 255, 0)',
          'rgba(0, 255, 255, 1)',
          'rgba(0, 191, 255, 1)',
          'rgba(0, 127, 255, 1)',
          'rgba(0, 63, 255, 1)',
          'rgba(0, 0, 255, 1)',
          'rgba(0, 0, 223, 1)',
          'rgba(0, 0, 191, 1)',
          'rgba(0, 0, 159, 1)',
          'rgba(0, 0, 127, 1)',
          'rgba(63, 0, 91, 1)',
          'rgba(127, 0, 63, 1)',
          'rgba(191, 0, 31, 1)',
          'rgba(255, 0, 0, 1)'
        ]
});

//toggle hospital heatmap
function toggle_heatmap_hospital() {
    heatMapHospital.setMap(heatMapHospital.getMap() ? null : map);
}

//init hospital heatmap toggle
var heatMapPoliceDept = new google.maps.visualization.HeatmapLayer({
    data: policeDept_coords,
    map: map,
    radius: 40,
    gradient: [
          'rgba(0, 255, 255, 0)',
          'rgba(0, 255, 255, 1)',
          'rgba(0, 191, 255, 1)',
          'rgba(0, 127, 255, 1)',
          'rgba(0, 63, 255, 1)',
          'rgba(0, 0, 255, 1)',
          'rgba(0, 0, 223, 1)',
          'rgba(0, 0, 191, 1)',
          'rgba(0, 0, 159, 1)',
          'rgba(0, 0, 127, 1)',
          'rgba(63, 0, 91, 1)',
          'rgba(127, 0, 63, 1)',
          'rgba(191, 0, 31, 1)',
          'rgba(255, 0, 0, 1)'
        ]
});

//toggle hospital heatmap
function toggle_heatmap_policeDept() {
    heatMapPoliceDept.setMap(heatMapPoliceDept.getMap() ? null : map);
}


// Adds a marker to the map.
function addMarker(location) {
    marker = new google.maps.Marker({
        position: location,
        map: map
    });
}

/////////////////
//calculate score
/////////////////
function calculateScore(marker) {
    var threshold = 3; //threshold in km for distance to a category

    //variables to keep track of different metrics
    var currentDistance = 0;
    var school_count = 0;
    var hospital_count = 0;
    var policeDept_count = 0;
    var streetlight_count = 0;
    var score = 0;

    //count number of schools within a 3 km radius
    for (var i = 0; i < schools.length; i++) {
        currentDistance = calcDistance(marker.lat(),marker.lng(),school_coords[i].lat(),school_coords[i].lng());
        if (currentDistance < threshold) { //within 3km radius
            school_count += 1;
        }
    }

    //count number of hospitals within a 3 km radius
    for (var i = 0; i < hospitals.length; i++) {
        currentDistance = calcDistance(marker.lat(),marker.lng(),hospital_coords[i].lat(),hospital_coords[i].lng());
        if (currentDistance < threshold) { //within 3km radius
            hospital_count += 1;
        }
    }

    //count number of police departments within a 3 km radius
    for (var i = 0; i < policeDept_coords.length; i++) {
        currentDistance = calcDistance(marker.lat(),marker.lng(),policeDept_coords[i].lat(),policeDept_coords[i].lng());
        if (currentDistance < threshold) { //within 3km radius
            policeDept_count += 1;
        }
    }

    //count number of streetlights within a 3 km radius
    for (var i = 0; i < streetlight_coords.length; i++) {
        currentDistance = calcDistance(marker.lat(),marker.lng(),streetlight_coords[i].lat(),streetlight_coords[i].lng());
        if (currentDistance < threshold) { //within 3km radius
            streetlight_count += 1;
        }
    }

    //we have to wait around 30 seconds for all the properties and crimes to load, otherwise the score will be negative due to our scaling
    console.log('policeDept count: ', policeDept_count,'school count: ', 
        school_count,'hospital count: ', hospital_count, 'streetlight count: ', streetlight_count);

    //calculate score based on the counts
    score = school_count * 0.25 + hospital_count * 0.25 - policeDept_count * 0.25 -  streetlight_count * 0.20;//scale score so it is positive due to the large number of residences and crimes

    //set the score in the HTML
    document.getElementById('score').innerHTML = 'Score: ' + score;
}

//compute the closest (school/hospital) to the marker
function getClosest(marker,category,type) {
    var minDistance = 100000;
    var currentDistance , counter;

    //compute closest distance so we only have to make one google API call
    for (var i = 0; i < category.length; i++) {
        currentDistance = calcDistance(marker.lat(),marker.lng(),category[i].lat(),category[i].lng());
        if (currentDistance < minDistance) { //found new minimum distance
            minDistance = currentDistance;
            counter = i;
        }
    }
    var service = new google.maps.DistanceMatrixService();
    service.getDistanceMatrix({
        origins: [marker],
        destinations: [category[counter]],
        travelMode: google.maps.TravelMode.DRIVING,
        unitSystem: google.maps.UnitSystem.IMPERIAL
    }, callback);

    //parse response and post to page
    function callback(response, status) {
        if (status == "OK") {

            document.getElementById('origin_address').innerHTML = 'Marker Address: ' + response.originAddresses;

            if (type == 'hospitals') {
                document.getElementById('hospital_name').innerHTML = 'Hospital Name: ' + hospitals[counter][0];
                document.getElementById('dest_address_hospital').innerHTML = 'Closest Hospital Address: ' + response.destinationAddresses;
                document.getElementById('distance_hospital').innerHTML = 'Distance to Hospital: ' + minDistance*1000 + ' meters';
            } else if (type == 'schools') {
                document.getElementById('school_name').innerHTML = 'School Name: ' + schools[counter][0];
                document.getElementById('dest_address_school').innerHTML = 'Closest School Address: ' + response.destinationAddresses;
                document.getElementById('distance_school').innerHTML = 'Distance to School: ' + minDistance*1000 + ' meters';
            }
            else if (type == 'policeDepts') {
                document.getElementById('policeDept_name').innerHTML = 'Police Department Name: ' + policeDepts[counter][0];
                document.getElementById('dest_address_policeDept').innerHTML = 'Closest Police Department Address: ' + response.destinationAddresses;
                document.getElementById('distance_policeDept').innerHTML = 'Distance to Police Department: ' + minDistance*1000 + ' meters';
            }
        } else {
            console.log(' Distance Matrix error status is: ' + status);
        }
    }

}

//calculates distance of two points
function calcDistance(lat1,lon1,lat2,lon2) {
    var R = 6371;
    var dLat = deg2rad(lat2-lat1);
    var dLon = deg2rad(lon2-lon1);
    var a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon/2) * Math.sin(dLon/2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    var d = R * c;
    return d;
}

//degree to radians
function deg2rad(deg){
    return deg * (Math.PI/180);
}

//////////////
// Old : Fetch data from Boston Data API directly, we are now using mongo to retrieve the data generated by our python files
// If mongo is not working correctly, uncomment the code below and it will retrieve the data from the API
//////////////

/*
var schools = fetchSchools();
var school_coords = fetchSchoolsCoord(); //just the coordinates
var hospitals = fetchHospitals(); //names and coord
var hospital_coords = fetchHospitalsCoord(); //just the coordinates
var crimes = fetchCrimesCoord();
var properties = fetchProperties();

//get hospital data in format: [name,coord]
function fetchHospitals(){
    var hospitals = [];
    $.getJSON('https://data.cityofboston.gov/api/views/46f7-2snz/rows.json?accessType=DOWNLOAD',{ },
    function(response) {
        for (var i = 0; i < 25; i++) {
            hospitals.push([ response.data[i][8] , [response.data[i][14][1],response.data[i][14][2]] ] );
        }
    });
    return hospitals;
}

//get hospital data in just coordinate form
function fetchHospitalsCoord(){
    var hospitals = [];
    $.getJSON('https://data.cityofboston.gov/api/views/46f7-2snz/rows.json?accessType=DOWNLOAD',{ },
    function(response) {
        for (var i = 0; i < 25; i++) {
            hospitals.push( new google.maps.LatLng(response.data[i][14][1],response.data[i][14][2]) );
        }
    });
    return hospitals;
}

//get school data in format: [name,coord]
function fetchSchools() {
    var schools = [];
    $.getJSON( 'https://data.cityofboston.gov/api/views/e29s-ympv/rows.json?accessType=DOWNLOAD',{ },
    function(response) {
        for (var i = 0; i < 25; i++) {
            schools.push( [response.data[i][10] , response.data[i][12][1] , response.data[i][12][2]] );
        }
    });
    return schools;
}
//get school data in just coordinate form
function fetchSchoolsCoord() {
    var schoolsCoord = [];
    $.getJSON( 'https://data.cityofboston.gov/api/views/e29s-ympv/rows.json?accessType=DOWNLOAD',{ },
    function(response) {
        for (var i = 0; i < 25; i++) {
            schoolsCoord.push( new google.maps.LatLng( response.data[i][12][1] , response.data[i][12][2] ) );
        }
    });
    return schoolsCoord;
}

function fetchCrimesCoord() {
    var crimes = [];
    $.getJSON( 'https://data.cityofboston.gov/api/views/fqn4-4qap/rows.json?accessType=DOWNLOAD',{ },
    function(response) {
        for (var i = 0; i < 500; i++) {
            crimes.push( new google.maps.LatLng( response.data[i][24][1] , response.data[i][24][2] ) );
        }
    });
    return crimes;
}

function fetchProperties() {
    console.log('running fetch properties');
    var residentUse = ['CD', 'R1', 'R2', 'R3', 'R4', 'RC', 'RL'];
    var properties = [];
    $.getJSON( 'https://data.cityofboston.gov/api/views/i7w8-ure5/rows.json?accessType=DOWNLOAD',{ },
    function(response) {
        for (var i = 0; i < response.data.length; i++) {
            //console.log('i: ' + i);
            if (response.data[i][83] == "#N/A" || response.data[i][84] == "#N/A") {
                ;
            } else if ( residentUse.indexOf(response.data[i][13]) != -1 ){
                properties.push( new google.maps.LatLng( response.data[i][83] , response.data[i][84] ) );
            }
        }

    });
    console.log(properties);
    return properties;
}
*/
