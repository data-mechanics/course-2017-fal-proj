$(document).ready(function(){
  document.getElementById('outermap2').innerHTML = '<div id="map2"></div>';
  var map2 = L.map("map2", {zoomControl: true}).setView([42.35, -71.08], 13);

  // load a tile layer
  L.tileLayer("http://{s}.tile.openstreetmap2.org/{z}/{x}/{y}.png",//"https://tiles.map2c.org/basemap2/{z}/{x}/{y}.png",
    {
      attribution: "CS 591 Data Mechanics",
      maxZoom: 15,
      minZoom: 10
    }).addTo(map2);

  map2.setZoom(11);


  var metricToReload = "regression"

  $.getJSON("regressionAnalysis.geojson", function (hoodData) {
    console.log(hoodData);

    var topo = L.geoJson(hoodData, {

      style: function (feature) {
        var fillColor,
        coeff = parseFloat(feature.properties[metricToReload]);
        val = (sigmoid(coeff/100) - 0.5) * 2;
        if (val > 0) {
          hex_value = rgb2hex("rgba(0," + Math.round(val * 255) + ",0,1)");
        } else if (val === 0) {
          hex_value = rgb2hex("rgba(255,255,255,1)");
        } else {
          hex_value = rgb2hex("rgba(" + Math.round(val * -255) + "0,0,1)");
        }

        fillColor = hex_value;

        return {color: "#999", weight: 1, fillColor: fillColor, fillOpacity: .6};
      },
      onEachFeature: function (feature, layer) {
        if (val === 0) {
          layer.bindPopup("<strong>" + feature.properties.Name + "</strong><br/> No Data");
        } else {
          layer.bindPopup("<strong>" + feature.properties.Name + "</strong><br/> Regression Coefficient: " + val);
        }
        layer.on('mouseover', function (e) {
            this.openPopup();
        });
        layer.on('mouseout', function (e) {
            this.closePopup();
        });
        layer.on({
          click: function(e) {
            var layer = e.target;
            if (layer.options.color == '#00FFFF') {
              topo.resetStyle(e.target);
              e.target.feature.properties.selected = false;
            } else {
              layer.setStyle({
                weight: 3,
                color: "#00FFFF",
                opacity: 1,
                fillOpacity: 0.1
              });
              e.target.feature.properties.selected = true;
            }
            getAllElements();
            if (!L.Browser.ie && !L.Browser.opera) {
                layer.bringToFront();
            }
          }
        });
      }
    }).addTo(map2);
  });

  // initialize the map2

  //Function to convert hex format to a rgb color
  function rgb2hex(rgb) {
    rgb = rgb.match(/^rgba?[\s+]?\([\s+]?(\d+)[\s+]?,[\s+]?(\d+)[\s+]?,[\s+]?(\d+)[\s+]?/i);
    return (rgb && rgb.length === 4) ? "#" +
    ("0" + parseInt(rgb[1], 10).toString(16)).slice(-2) +
    ("0" + parseInt(rgb[2], 10).toString(16)).slice(-2) +
    ("0" + parseInt(rgb[3], 10).toString(16)).slice(-2) : "";
  }

  function sigmoid(t) {
    return 1/(1+Math.pow(Math.E, -t));
  }
  function getAllElements() {
    var selectedFeatureName = [];
    var coeffTotal = 0;
    $.each(map2._layers, function (ml) {
        if (map2._layers[ml].feature && map2._layers[ml].feature.properties.selected === true) {
          selectedFeatureName.push(map2._layers[ml].feature.properties.Name);
          coeffTotal += parseFloat(map2._layers[ml].feature.properties.regression);
        }
    });
    var finalCoeff = Math.round((sigmoid(coeffTotal/100))*100,2)+"%"
    $('#selected').text( selectedFeatureName.join(", ") );
    $('#reliability').text(finalCoeff);
  };
});