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
          {lat: 42.3552843449157, lng: -71.0723848018002},
          {lat: 42.3550426827642, lng: -71.0723675227058},
          {lat: 42.3547601567921, lng: -71.072116768774},
          {lat: 42.3546468232862, lng: -71.072061653538},
          {lat: 42.3542674526538, lng: -71.0718773857171},
          {lat: 42.3538378877772, lng: -71.0716622463931}
    ];

    var highlightPath = new google.maps.Polyline({
          path: highlightTest,
          strokeColor: '#000000',
          strokeOpacity: 25.0,
          strokeWeight: 5
    });

    highlightPath.setMap(map);

	heatmap.set('radius', 200);
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