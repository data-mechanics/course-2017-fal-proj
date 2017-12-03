var map, heatmap;

//Function automatically called by script call in <head> of heatmap.html

function initMap() {
	
	map = new google.maps.Map(document.getElementById('map'), {
		zoom: 15,
		center: {lat: 42.3601, lng: -71.0589},
		mapTypeId: 'roadmap'
	});

	heatmap = new google.maps.visualization.HeatmapLayer({
		data: kMeansData(),
		map: map
	});

	var highlightTest = [
          {lat: 42.2993795717675, lng: -71.0736591599471},
          {lat: 42.299423376192, lng: -71.0734870202808},
          {lat: 42.2994785961146, lng: -71.0732700178509},
          {lat: 42.2994915969646, lng: -71.0732068528117},
          {lat: 42.2995786734218, lng: -71.0729614959582},
          {lat: 42.2997187669002, lng: -71.0726311018849},
          {lat: 42.29977963417, lng: -71.0725094340628},
          {lat: 42.2998579145923, lng: -71.0724031193472},
          {lat: 42.2999172568774, lng: -71.0723231845583},
          {lat: 42.2999719175878, lng: -71.072263672069},

          {lat: 42.3000142817983, lng: -71.0722181041279},
          {lat: 42.300068288634, lng: -71.0721671427019},
          {lat: 42.3001250879832, lng: -71.0721293317132},
          {lat: 42.3001922083806, lng: -71.0720995381885},
          {lat: 42.3003203509277, lng: -71.0720605513052},
          {lat: 42.3003923439571, lng: -71.0720496682155},
          {lat: 42.300468480873, lng: -71.0720463812799},
          {lat: 42.300680468067, lng: -71.072047489801},
          {lat: 42.3007072240428, lng: -71.0720477709737},
          {lat: 42.301255363582, lng: -71.0720477628888}
    ];

    var highlightPath = new google.maps.Polyline({
          path: highlightTest,
          strokeColor: '#00FFFF',
          strokeOpacity: 25.0,
          strokeWeight: 5
    });

    highlightPath.setMap(map);
    initHeatMap();
}

/*
function findDistance(lat1, lon1, lat2, lon2) {

	var distance = Math.sqrt(Math.pow((lat1 - lat2), 2) + Math.pow((lon1 - lon2), 2))
	return distance

}

function findClosestCentroid(lat1, lon1, kMeansData) {
	var min = Number.MAX_SAFE_INTEGER;
	
	for (i = 0; i < kMeansData.length; i++) {
		var nextDist = findDistance(lat1, lon1, kMeansData[i][0], kMeansData[i][1]);
		if (nextDist < min) {
			min = nextDist;
		}
	}

	return min;
}*/

function kMeansData() {
	return [
		new google.maps.LatLng(42.3340820819,
		-71.10586771785),
		new google.maps.LatLng(42.3495376231,
		-71.1419411892),
		new google.maps.LatLng(42.331648631075,
		-71.098239928575),
		new google.maps.LatLng(42.33046328958,
		-71.1070608519),
		new google.maps.LatLng(42.3360449123,
		-71.10826387746667),
		new google.maps.LatLng(42.3358606402,
		-71.0996350065),
		new google.maps.LatLng(42.3395648043,
		-71.1026941935),
		new google.maps.LatLng(42.3268347904,
		-71.099189638),
		new google.maps.LatLng(42.3346188381,
		-71.10290921076667)
	];
}

function initHeatMap() {
	heatmap.set('radius', 200);
	var gradient = [
    'rgba(0, 255, 255, 0)',
    'rgba(15, 235, 235, 1)',
    'rgba(55, 205, 205, 1)',
    'rgba(95, 185, 205, 1)',
    'rgba(215, 215, 235, 1)',
    'rgba(195, 195, 215, 1)',
    'rgba(185, 185, 205, 1)',
    'rgba(125, 125, 135, 1)'
  ]
  heatmap.set('gradient',gradient);
}