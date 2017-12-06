
let map;
let hospitalMarkers = []
let clusterMarkers = []
let crimeMarkers = []
let policeMarkers = []
let trafficMarkers= []
let optimalcoordMarkers = []
let mbtaMarkers = []


function initialize() {
	var boston = { lat: 42.3601, lng: -71.0589 };
	map = new google.maps.Map(document.getElementById('map'), {
  		zoom: 11,
  		center: boston
		});
	generateCheckboxes()
}

function generateCheckboxes(){
	const checkBoxes = ["hospitals","crimes","mbtastops","policestations","optimalcoords","trafficlocs","clusters"]
	const checkBoxNames = ["Hospitals","Crime Locations","Mbta Stops","Police Stations","Optimal Coord","traffic locations","Clusters"]
	for(let c=0; c < checkBoxes.length; c++){
		document.getElementById("checkboxes").innerHTML += "<label><input type='checkbox' onclick='updateMapMarkers(this);' name ='"+checkBoxes[c]+"' value='"+checkBoxes[c]+"'>"+checkBoxNames[c]+"</label><br>"
	}
}

function updateMapMarkers(cb){
	url="http://localhost:3000/api/"+cb.getAttribute('value')
	console.log(cb.getAttribute('value') )
	switch(cb.getAttribute('value')){
		case 'hospitals':
			if(cb.checked){
				if(hospitalMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						addHospitalMarkers(data[cb.getAttribute('value')],map)
					})
				}else{
					hospitalMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				hospitalMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
		
		case 'crimes':
			if(cb.checked){
				if(crimeMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						addCrimeMarkers(data[cb.getAttribute('value')],map)
					})
				}else{
					crimeMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				crimeMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
		case 'mbtastops':
			if(cb.checked){
				if(mbtaMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						addMbtaMarkers(data['mbta_stops'],map)
					})
				}else{
					mbtaMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				mbtaMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
		case 'policestations':
			if(cb.checked){
				if(policeMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						addPoliceStationMarkers(data[cb.getAttribute('value')],map)
					})
				}else{
					policeMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				policeMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
		case 'optimalcoords':
			if(cb.checked){
				if(optimalcoordMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						console.log(data)
						addOptimalCoordsMarker(data['optimalcoord'],map)
					})
				}else{
					optimalcoordMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				optimalcoordMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
		case 'trafficlocs':
			if(cb.checked){
				if(trafficMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						addtrafficlocsMarkers(data[cb.getAttribute('value')],map)
					})
				}else{
					trafficMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				trafficMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
		case 'clusters':
			if(cb.checked){
				if(clusterMarkers.length === 0){
					fetch(url)
					.then(res => res.json())
					.then(function(data){ 
						addClusterMarkers(data['c_clusters'],map)
						addClusterMarkers(data['f_clusters'],map)
					})
				}else{
					clusterMarkers.forEach(function(marker){
						marker.setMap(map)
					})
				}
			}else{
				clusterMarkers.forEach(function(marker){
					marker.setMap(null)
				})
			}
			break;
	}
}

function addOptimalCoordsMarker(optimalcoord,map){
	optimalcoordMarkers.push(addMarker('',{lat:parseFloat(optimalcoord[0].optimal_coord[0]),lng:parseFloat(optimalcoord[0].optimal_coord[1])},map,'blue',"Optimal Hospital Location"))
}
function addtrafficlocsMarkers(trafficlocs,map){
	for(var i = 0; i < trafficlocs.length; i++){
		trafficMarkers.push(addMarker('',{lat:parseFloat(trafficlocs[i].location[0]),lng:parseFloat(trafficlocs[i].location[1])},map,'red',"Traffic Accident"))
	}
}
function addPoliceStationMarkers(policestations,map){
	for(var i = 0; i < policestations.length; i++){
		policeMarkers.push(addMarker('',{lat:parseFloat(policestations[i].location[0]),lng:parseFloat(policestations[i].location[1])},map,'red',policestations[i].identifier))
	}
}
function addMbtaMarkers(mbtaStops,map){
	for(var i = 0; i < mbtaStops.length; i++){
		mbtaMarkers.push(addMarker('',{lat:parseFloat(mbtaStops[i].location[0]),lng:parseFloat(mbtaStops[i].location[1])},map,'green',mbtaStops[i].identifier))
	}
}
function addCrimeMarkers(crimeData,map){
	for(var i = 0; i < crimeData.length; i++){
		crimeMarkers.push(addMarker('',{lat:parseFloat(crimeData[i].location[0]),lng:parseFloat(crimeData[i].location[1])},map,'red',crimeData[i].identifier))
	}
}

function addHospitalMarkers(hospitalData,map){
	for(var i = 0; i < hospitalData.length; i++){
		hospitalMarkers.push(addMarker('',{lat:hospitalData[i].location[0],lng:hospitalData[i].location[1]}, map,'yellow',hospitalData[i].identifier));
	}
}

function addClusterMarkers(clusters, map){
	let color = ""
  for(let i = 0; i < clusters.length; i++){
  	if(clusters[i].proximity == "F"){
  		color = 'red'
  	}else{
  		color = 'green'
  	}
  	clusterMarkers.push(addMarker('',{lat:clusters[i].location[0],lng:clusters[i].location[1]}, map,color,"cluster"))
  }
}

// Adds a marker to the map.
function addMarker(locationName,location, map,color,name) {
	// Add the marker at the clicked location, and add the next-available label
	// from the array of alphabetical characters.
	var marker = new google.maps.Marker({
	  position: location,
	  label: locationName,
	  map: map
	});
	marker.setIcon("http://labs.google.com/ridefinder/images/mm_20_".concat(color,'.png'))
	
	var infowindow = new google.maps.InfoWindow({
          content: "<div class='infoWindow'>"+name+"</div>"
        });

	marker.addListener('mouseover', function() {
    infowindow.open(map, this);
	});
	marker.addListener('mouseout', function() {
    infowindow.close();
	});
	return marker
}

google.maps.event.addDomListener(window, 'load', initialize);