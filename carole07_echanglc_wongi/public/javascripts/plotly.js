//retrieve data from mongoDB

var neighborhood_scores = JSON.parse(document.getElementById('neighborhood_scores').innerHTML);
//console.log('neighborhood_scores' , neighborhood_scores);
//console.log(neighborhood_scores[0][5])


//Generates the plotly graph

//STREETLIGHTS
var streetlight_trace = {
  x: ['Allston', 'Back Bay', 'Bay Village', 'Beacon Hill', 'Brighton', 'Charlestown', 'Chinatown', 'Dorchester', 'Downtown Crossing', 'East Boston', 'Fenway', 'Hyde Park', 'Jamaica Plain', 'Mattapan', 'Mission Hill', 'North End', 'Roslindale', 'Roxbury', 'South Boston', 'West End', 'West Roxbury'],
  y: [ neighborhood_scores[0][2]*.01 , neighborhood_scores[1][2]*.01 , neighborhood_scores[2][2]*.01 , neighborhood_scores[3][2]*.01 , neighborhood_scores[4][2]*.01 , neighborhood_scores[5][2]*.01 , neighborhood_scores[6][2]*.01 , neighborhood_scores[7][2]*.01 , neighborhood_scores[8][2]*.01 , neighborhood_scores[9][2]*.01 ,neighborhood_scores[10][2]*.01 , neighborhood_scores[11][2]*.01 , neighborhood_scores[12][2]*.01 , neighborhood_scores[13][2]*.01 , neighborhood_scores[14][2]*.01 , neighborhood_scores[15][2]*.01 , neighborhood_scores[16][2]*.01 , neighborhood_scores[17][2]*.01 , neighborhood_scores[18][2]*.01 , neighborhood_scores[19][2]*.01 , neighborhood_scores[20][2]*.01 , neighborhood_scores[21][2]*.01 ] ,
  //y: [828954*.01, 2389486*.01, 2055781*.01, 2460397*.01, 750061*.01, 1466931*.01, 1948693*.01,577924*.01, 1835967*.01, 1423304*.0001, 1360339*.01, 335695*.01, 688737*.01, 350038*.01, 862229*.01, 1707763*.01, 410828*.01, 516787*.01, 1614672*.01, 1399858*.0001, 2036205*.01, 422383*.01],
  type: 'scatter'
};

// POLICE DEPTS RATE
var policeDept_trace = {
  x: ['Allston', 'Back Bay', 'Bay Village', 'Beacon Hill', 'Brighton', 'Charlestown', 'Chinatown', 'Dorchester', 'Downtown Crossing', 'East Boston', 'Fenway', 'Hyde Park', 'Jamaica Plain', 'Mattapan', 'Mission Hill', 'North End', 'Roslindale', 'Roxbury', 'South Boston', 'West End', 'West Roxbury'],
  y: [ neighborhood_scores[0][3] , neighborhood_scores[1][3], neighborhood_scores[2][3], neighborhood_scores[3][3], neighborhood_scores[4][3], neighborhood_scores[5][3], neighborhood_scores[6][3], neighborhood_scores[7][3], neighborhood_scores[8][3], neighborhood_scores[9][3], neighborhood_scores[10][3], neighborhood_scores[11][3], neighborhood_scores[12][3], neighborhood_scores[13][3], neighborhood_scores[14][3], neighborhood_scores[15][3], neighborhood_scores[16][3], neighborhood_scores[17][3], neighborhood_scores[18][3], neighborhood_scores[19][3], neighborhood_scores[20][3], neighborhood_scores[21][3] ],
  //y: [9960, 46910, 48982, 39728, 7844, 20714, 46512, 46090, 42214, 21971, 37248, 9561, 32559, 24580, 36950, 31544, 13138, 52097, 41100, 59815, 34357, 4652],
  type: 'scatter'
};

// HOSPITAL DISTANCE
var hospital_trace = {
    x: ['Allston', 'Back Bay', 'Bay Village', 'Beacon Hill', 'Brighton', 'Charlestown', 'Chinatown', 'Dorchester', 'Downtown Crossing', 'East Boston', 'Fenway', 'Hyde Park', 'Jamaica Plain', 'Mattapan', 'Mission Hill', 'North End', 'Roslindale', 'Roxbury', 'South Boston', 'West End', 'West Roxbury'],
    y: [ neighborhood_scores[0][4] , neighborhood_scores[1][4], neighborhood_scores[2][4], neighborhood_scores[3][4], neighborhood_scores[4][4], neighborhood_scores[5][4], neighborhood_scores[6][4], neighborhood_scores[7][4], neighborhood_scores[8][4], neighborhood_scores[9][4], neighborhood_scores[10][4], neighborhood_scores[11][4], neighborhood_scores[12][4], neighborhood_scores[13][4], neighborhood_scores[14][4], neighborhood_scores[15][4], neighborhood_scores[16][4], neighborhood_scores[17][4], neighborhood_scores[18][4], neighborhood_scores[19][4], neighborhood_scores[20][4], neighborhood_scores[21][4] ],
    //y: [0.16001979257907314, 1.0202819982628333, 0.5354757984150593, 0.42570246403084794, 1.2437650755569514, 1.2557752685062529, 0.08482466653796399, 1.4124338532760736, 0.6999797624245208, 2.215623734141166, 0.6931341777907019, 3.8629676782088493, 1.436916128020817, 1.0218596535964761, 0.0028661666637534314, 0.995959889303452, 1.6201146479126376, 0.3745273172783064, 1.8503431238923769, 0.38988065384160736, 0.13931661442378618, 0.9073310312990648],
    type: 'scatter'
};

// SCHOOL COUNT
var school_trace = {
    x: ['Allston', 'Back Bay', 'Bay Village', 'Beacon Hill', 'Brighton', 'Charlestown', 'Chinatown', 'Dorchester', 'Downtown Crossing', 'East Boston', 'Fenway', 'Hyde Park', 'Jamaica Plain', 'Mattapan', 'Mission Hill', 'North End', 'Roslindale', 'Roxbury', 'South Boston', 'West End', 'West Roxbury'],
    y: [ neighborhood_scores[0][5] , neighborhood_scores[1][5], neighborhood_scores[2][5], neighborhood_scores[3][5], neighborhood_scores[4][5], neighborhood_scores[5][5], neighborhood_scores[6][5], neighborhood_scores[7][5], neighborhood_scores[8][5], neighborhood_scores[9][5], neighborhood_scores[10][5], neighborhood_scores[11][5], neighborhood_scores[12][5], neighborhood_scores[13][5], neighborhood_scores[14][5], neighborhood_scores[15][5], neighborhood_scores[16][5], neighborhood_scores[17][5], neighborhood_scores[18][5], neighborhood_scores[19][5], neighborhood_scores[20][5], neighborhood_scores[21][5] ],
    //y: [11, 26, 29, 19, 10, 13, 24, 39, 25, 13, 30, 15, 27, 25, 38, 19, 26, 45, 23, 41, 18, 10],
    type: 'scatter'
};

var streetlight_data = [streetlight_trace];
var hospital_data = [hospital_trace];
var school_data = [school_trace];
var policeDept_data = [policeDept_trace];

var streetlight_layout = {
    title: ' # of Streetlights in Each Neighborhood'
}

var hospital_layout = {
    title: ' # of Hospitals in Each Neighborhood'
}

var school_layout = {
    title: ' # of Schools in Each Neighborhood'
}

var policeDept_layout = {
    title: ' # of Police Stations in Each Neighborhood'
}

Plotly.newPlot('streetlight_graph', streetlight_data,streetlight_layout);
Plotly.newPlot('hospital_graph', hospital_data,hospital_layout);
Plotly.newPlot('school_graph', school_data,school_layout);
Plotly.newPlot('policeDept_graph', policeDept_data,policeDept_layout);

$(document).ready(function(){
    $("#streetlight_graph_toggle").click(function(){
        //console.log('property toggle clicked');
        $("#streetlight_graph").show();
        $("#school_graph").hide();
        $("#hospital_graph").hide();
        $("#policeDept_graph").hide();
    });

    $("#school_graph_toggle").click(function(){
        //console.log('property toggle clicked');
        $("#school_graph").show();
        $("#streetlight_graph").hide();        
        $("#hospital_graph").hide();
        $("#policeDept_graph").hide();
    });

    $("#policeDept_graph_toggle").click(function(){
        //console.log('property toggle clicked');
        $("#policeDept_graph").show();
        $("#school_graph").hide();
        $("#hospital_graph").hide();
        $("#streetlight_graph").hide();
    });

    $("#hospital_graph_toggle").click(function(){
        //console.log('property toggle clicked');
        $("#hospital_graph").show();
        $("#school_graph").hide();
        $("#streetlight_graph").hide();
        $("#policeDept_graph").hide();
    });

});
