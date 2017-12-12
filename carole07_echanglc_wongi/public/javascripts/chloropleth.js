//generates the chloropleth graph

var neighborhood_scores = JSON.parse(document.getElementById('neighborhood_scores').innerHTML);
//console.log('neighborhood_scores:' , neighborhood_scores);


// THIS IS THE JSON OBJECT W/ ALL TH DATA IN IT.
var zipData = {
    "type": "FeatureCollection", "features": [
    {
        "type": "Feature",
        "id": "01",
        "properties": {
            "name": "Allston",
            "density": neighborhood_scores[0][1].toFixed(2) //4.77 // The density is the score.
        },
        "geometry":{
            "type": "Polygon",
            "coordinates": [[[-71.110643, 42.352749], [-71.125342,42.351723], [-71.135254,42.345912], [-71.135254,42.364029], [-71.122949,42.369156], [-71.117138,42.36437], [-71.110643,42.352749]]]
        }
    },
    {
        "type": "Feature",
        "id" : "02",
        "properties": {
            "name": "Back Bay",
            "density": neighborhood_scores[1][1].toFixed(2) //1.64
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.064498,42.34728], [-71.080221,42.344203], [-71.081931,42.34557], [-71.08364,42.348305], [-71.089792,42.351381], [-71.09116,42.354116], [-71.085349,42.356167], [-71.077145,42.358901], [-71.073385,42.355825], [-71.064498,42.352407], [-71.064498,42.34728]]]
        }
    },
      /* TOOK BAY VILLAGE OUT DUE TO UNCLEAR COORDINATES
    {
        "type": "Feature",
        "id": "03",
        "properties": {
            "name": "Bay Village",
            "density": 50.24
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": []
        }
    },*/
    {
        "type": "Feature",
        "id": "04",
        "properties": {
            "name": "Beacon Hill",
            "density": neighborhood_scores[3][1].toFixed(2) //1.6
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.060396,42.355483], [-71.064498,42.352407], [-71.073385,42.355825], [-71.057661,42.363003], [-71.058687,42.35685], [-71.060396,42.355483]]]
        }
    },
    {
        "type": "Feature",
        "id": "05",
        "properties": {
            "name": "Brighton",
            "density": neighborhood_scores[4][1].toFixed(2) //5.28
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.135254,42.345912], [-71.139698,42.342152], [-71.149952,42.335316], [-71.169094,42.342836], [-71.161916,42.359243], [-71.1438,42.365054], [-71.13628,42.366763], [-71.135254,42.364029], [-71.135254,42.345912]]]
        }
    },
    {
        "type": "Feature",
        "id": "06",
        "properties": {
            "name": "Charlestown",
            "density": neighborhood_scores[5][1].toFixed(2) //2.69
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.065181,42.369498], [-71.073043,42.373258], [-71.080905,42.382145], [-71.074411,42.390691], [-71.065181,42.369498]]]
        }
    },
    {
        "type": "Feature",
        "id": "07",
        "properties": {
            "name": "Chinatown",
            "density": neighborhood_scores[6][1].toFixed(2)// 2.0
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.064498,42.34728], [-71.064498,42.352407], [-71.060396,42.355483], [-71.055269,42.352749], [-71.052876,42.351381], [-71.060738,42.34557], [-71.064498,42.34728]]]
        }
    },
    {
        "type": "Feature",
        "id": "08",
        "properties": {
            "name": "Dorchester",
            "density": neighborhood_scores[7][1].toFixed(2)//6.41
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.09868,42.29669], [-71.097654,42.31173], [-71.078854,42.314465], [-71.069967,42.305578], [-71.072018,42.301818], [-71.09868,42.29669]]]
        }
    },
    {
        "type": "Feature",
        "id": "09",
        "properties": {
            "name": "Downtown Crossing",
            "density": neighborhood_scores[8][1].toFixed(2)//2.13
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.060396,42.355483], [-71.064498,42.352407], [-71.073385,42.355825], [-71.057661,42.363003], [-71.058687,42.35685], [-71.060396,42.355483]]]
        }
    },
    {
        "type": "Feature",
        "id": "10",
        "properties": {
            "name": "East Boston",
            "density": neighborhood_scores[9][1].toFixed(2)//2.77
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.011174,42.396501], [-70.986221,42.388298], [-70.994083,42.382829], [-71.041938,42.36437], [-71.011174,42.396501]]]
        }
    },
    {
        "type": "Feature",
        "id": "11",
        "properties": {
            "name": "Fenway",
            "density": neighborhood_scores[10][1].toFixed(2)//2.86
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.110985,42.332581], [-71.113378,42.331898], [-71.110985,42.335316], [-71.089792,42.351381], [-71.08364,42.348305], [-71.081931,42.34557], [-71.080221,42.344203], [-71.084665,42.340443], [-71.110985,42.332581]]]
        }
    },
    {
        "type": "Feature",
        "id": "12",
        "properties": {
            "name": "Hyde Park",
            "density": neighborhood_scores[11][1].toFixed(2)//11.59
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.108592,42.261141], [-71.130811,42.227643], [-71.150294,42.257381], [-71.140723,42.27413], [-71.112352,42.272079], [-71.108592,42.261141]]]
        }
    },
    {
        "type": "Feature",
        "id": "13",
        "properties": {
            "name": "Jamaica Plain",
            "density": neighborhood_scores[12][1].toFixed(2)//5.55
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.09868,42.29669], [-71.096629,42.293272], [-71.130469,42.298058], [-71.14004,42.302159], [-71.113378,42.331898], [-71.110985,42.332581], [-71.100047,42.322669], [-71.097654,42.31173], [-71.09868,42.29669]]]
        }
    },
    {
        "type": "Feature",
        "id": "14",
        "properties": {
            "name": "Mattapan",
            "density": neighborhood_scores[13][1].toFixed(2)//10.68
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.108592,42.261141], [-71.112352,42.272079], [-71.095945,42.291905], [-71.067916,42.270712], [-71.108592,42.261141]]]
        }
    },
    {
        "type": "Feature",
        "id": "15",
        "properties": {
            "name": "Mission Hill",
            "density": neighborhood_scores[14][1].toFixed(2)//4.45
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.100047,42.322669], [-71.110985,42.332581], [-71.084665,42.340443], [-71.081931,42.33429], [-71.100047,42.322669]]]
        }
    },
    {
        "type": "Feature",
        "id": "16",
        "properties": {
            "name": "North End",
            "density": neighborhood_scores[15][1].toFixed(2)//2.3
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.058003,42.363687], [-71.05937,42.367789], [-71.058687,42.367789], [-71.058003,42.363687]]]
        }
    },
    {
        "type": "Feature",
        "id": "17",
        "properties": {
            "name": "Roslindale",
            "density": neighborhood_scores[16][1].toFixed(2)//9.44
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.140723,42.27413], [-71.145167,42.29669], [-71.130469,42.298058], [-71.096629,42.293272], [-71.095945,42.291905], [-71.112352,42.272079], [-71.140723,42.27413]]]
        }
    },
    {
        "type": "Feature",
        "id": "18",
        "properties": {
            "name": "Roxbury",
            "density": neighborhood_scores[17][1].toFixed(2)//7.03
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.078854,42.314465], [-71.097654,42.31173], [-71.100047,42.322669], [-71.081931,42.33429], [-71.066549,42.326087], [-71.078854,42.314465]]]
        }
    },
    {
        "type": "Feature",
        "id": "19",
        "properties": {
            "name": "South Boston",
            "density": neighborhood_scores[18][1].toFixed(2)//2.42
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.04604,42.323352], [-71.061421,42.33053], [-71.060738,42.34557], [-71.052876,42.351381], [-71.040912,42.339076], [-71.04604,42.323352]]]
        }
    },
    {
        "type": "Feature",
        "id": "20",
        "properties": {
            "name": "South End",
            "density": neighborhood_scores[19][1].toFixed(2)//2.7
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.060738,42.34557], [-71.061421,42.33053], [-71.066549,42.326087], [-71.081931,42.33429], [-71.084665,42.340443], [-71.080221,42.344203], [-71.064498,42.34728], [-71.060738,42.34557]]]
        }
    },
    {
        "type": "Feature",
        "id": "21",
        "properties": {
            "name": "West End",
            "density": neighborhood_scores[20][1].toFixed(2)//1.93
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.077145,42.358901], [-71.070651,42.368131], [-71.070651,42.368131], [-71.05937,42.367789], [-71.058003,42.363687], [-71.057661,42.363003], [-71.073385,42.355825], [-71.077145,42.358901]]]
        }
    },
    {
        "type": "Feature",
        "id": "22",
        "properties": {
            "name": "West Roxbury",
            "density": neighborhood_scores[21][1].toFixed(2)//9.37
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-71.140723,42.27413], [-71.150294,42.257381], [-71.188236,42.279941], [-71.191654,42.282334], [-71.173538,42.297716], [-71.145167,42.29669], [-71.140723,42.27413]]]
        }
    }

    ]
};
// initialize the map
//ar mymap = L.map('mapid').setView([42.35, -71.08], 13);

// load a tile layer
var mapboxAccessToken = 'pk.eyJ1Ijoid29uZ2kiLCJhIjoiY2phdzAzOWt5MGVqcjJxc2h3d204amhhdiJ9.Tb2_3RWpipJIOjKtVmQb5Q';
var map = L.map('mapid').setView([42.35, -71.08], 11.3);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=' + mapboxAccessToken, {
    id: 'mapbox.light',
    attribution: 'carole07_echanglc_wongi'
}).addTo(map);

function getColor(d) {
return d > 10 ? '#800026' : // Higher scores
       d > 6  ? '#BD0026' :
       d > 3  ? '#E31A1C' :
       d > 2  ? '#FC4E2A' :
       d > 0   ? '#FD8D3C' : // Lower scores
                  '#FFEDA0';
}

function style(feature) {
return {
    fillColor: getColor(feature.properties.density),
    weight: 2,
    opacity: 1,
    color: 'white',
    dashArray: '3',
    fillOpacity: 0.7
    };
}

//L.geoJson(zipData).addTo(map);

L.geoJson(zipData, {style: style}).addTo(map);


var geojson;
//geojson = L.geoJson(zipData, {style: style}).addTo(map);

// Event listener for mouseover event
function highlightFeature(e) {
    var layer = e.target;

    layer.setStyle({
        weight: 5,
        color: '#666',
        dashArray: '',
        fillOpacity: 0.7
    });

    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }
  info.update(layer.feature.properties);
}

// Mouseout ...
function resetHighlight(e) {
    geojson.resetStyle(e.target);
    info.update();
}

// Click listener...
function zoomToFeature(e) {
    map.fitBounds(e.target.getBounds());
}

// Add the listeners on our state layers
function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: zoomToFeature
    });
}

geojson = L.geoJson(zipData, {
    style: style,
    onEachFeature: onEachFeature
}).addTo(map);

/* CODE FOR OUR CONTROL */
var info = L.control();

info.onAdd = function (map) {
  this._div = L.DomUtil.create('div', 'info'); // create a div with a class "info"
  this.update();
  return this._div;
};

// method that updates the control based on feature properties passed
info.update = function (props) {
  this._div.innerHTML = '<h4>Boston Neighborhood Scores</h4>' + (props ?
      '<b>' + props.name + '</b><br />' + props.density
      : 'Hover over a neighborhood');
};

info.addTo(map);

/* CODE FOR THE LEGEND */
var legend = L.control({position: 'bottomright'});

legend.onAdd = function (map) {

var div = L.DomUtil.create('div', 'info legend'),
    grades = [0,2,3,6,10],
    labels = [];

    // loop through our density intervals and generate a label with a colored square for each interval
    for (var i = 0; i < grades.length; i++) {
        div.innerHTML +=
            '<i style="background:' + getColor(grades[i] + 1) + '"></i> ' +
            grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
    }

    return div;
};

legend.addTo(map);
