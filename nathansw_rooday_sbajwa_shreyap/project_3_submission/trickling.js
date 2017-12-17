$(document).ready(function(){
  var map = L.map("map", {zoomControl: false}).setView([42.35, -71.08], 13);

  // load a tile layer
  L.tileLayer("http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",//"https://tiles.mapc.org/basemap/{z}/{x}/{y}.png",
    {
      attribution: "CS 591 Data Mechanics",
      maxZoom: 15,
      minZoom: 10
    }).addTo(map);
  map.setZoom(11);


  $("select")
    .change(function () {

      document.getElementById('outermap').innerHTML = '<div id="map"></div>';

      var map = L.map("map", {zoomControl: true}).setView([42.35, -71.08], 13);

      // load a tile layer
      L.tileLayer("http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",//"https://tiles.mapc.org/basemap/{z}/{x}/{y}.png",
        {
          attribution: "CS 591 Data Mechanics",
          maxZoom: 15,
          minZoom: 10
        }).addTo(map);

      map.setZoom(11);


      var metricToReload = $('#metric :selected')[0].value;
      var metricText = $('#metric :selected').text();

      $.getJSON("neighborhoods_leaflet_new.geojson", function (hoodData) {
        console.log(hoodData);

        L.geoJson(hoodData, {

          style: function (feature) {
            var fillColor,
            density = feature.properties[metricToReload];
            console.log("density, metricToReload", density, metricToReload);

            red_value = Math.round((parseFloat(density) * 175) + 50);
            hex_value = rgb2hex("rgba(" + red_value + "0,0,1)");

            // console.log(hex_value)
            // console.log("hexvalue",hex_value , "*");

            fillColor = hex_value;

            return {color: "#999", weight: 1, fillColor: fillColor, fillOpacity: .6};
          },
          onEachFeature: function (feature, layer) {
            layer.bindPopup("<strong>" + feature.properties.Name + "</strong><br/>" + metricText + " trickling effect: " + feature.properties[metricToReload]);
          }
        }).addTo(map);
      });


    })
    .change();

  // initialize the map

  //Function to convert hex format to a rgb color
  function rgb2hex(rgb) {
    rgb = rgb.match(/^rgba?[\s+]?\([\s+]?(\d+)[\s+]?,[\s+]?(\d+)[\s+]?,[\s+]?(\d+)[\s+]?/i);
    return (rgb && rgb.length === 4) ? "#" +
    ("0" + parseInt(rgb[1], 10).toString(16)).slice(-2) +
    ("0" + parseInt(rgb[2], 10).toString(16)).slice(-2) +
    ("0" + parseInt(rgb[3], 10).toString(16)).slice(-2) : "";
  }
});