var saver = {};
var whomap;

function drawHist(data,special){
  var div = d3.select("body")
              .append("div")
              .attr("class", "toolTip")
              .attr("id","histo");

  var axisMargin = 30,
      margin = 10,
      valueMargin = 4,
      width = 620,
      height = 1000,
      barHeight = (height-axisMargin-margin*2)* 0.6/data.length,
      barPadding = (height-axisMargin-margin*2)*0.4/data.length,
      data, bar, svg, scale, xAxis, labelWidth = 0;

  max = d3.max(data, function(d) { return d.value; });

  svg = d3.select('body')
          .append("svg")
          .attr("width", width)
          .attr("height", height);


  bar = svg.selectAll("g")
           .data(data)
           .enter()
           .append("g");

  bar.attr("class", "bar")
          .attr("cx",0)
          .attr("transform", function(d, i) {
            return "translate(" + margin + "," + (i * (barHeight + barPadding) + barPadding) + ")";
          });

  bar.append("text")
     .attr("class", "label")
     .attr("y", barHeight / 2)
     .attr("dy", ".35em") //vertical align middle
     .attr('fill', 'black')
     .text(function(d){
       return d.label;
     }).each(function() {
       labelWidth = Math.ceil(Math.max(labelWidth, this.getBBox().width));
     });

  scale = d3.scale.linear()
            .domain([0, max])
            .range([0, width - margin*2 - labelWidth]);

  xAxis = d3.svg.axis()
            .scale(scale)
            .tickSize(-height + 2*margin + axisMargin)
            .orient("bottom");


  bar.append("rect")
      .attr("transform", "translate("+labelWidth+", 0)")
      .attr("height", barHeight)
      .attr("fill", function(d) {
        if(special == ""){
          return "#fddd9b";
        }
        else{
          if (d.label == special) {
            return "#ff9f77";
          }
          else {
            return "#fddd9b";
          }
        }
      })
      .attr("width", function(d){
          return scale(d.value);
      });



  svg.append("text")
      .attr("x", (width / 2))
      .attr("y", 0)
      .attr("text-anchor", "middle")
      .style("font-size", "20px")
      .text("Tope 50 Schools");

  svg.selectAll("rect")
      .on("mouseover", function(d){
        d3.select(this).attr("fill","red");
      })
      .on("mouseout", function(d){
        d3.select(this).attr("fill","#fddd9b");
      })
  bar.on("mouseover", function(d){
      div.style("left", d3.event.pageX+10+"px");
      div.style("top", d3.event.pageY-25+"px");
      div.style("display", "inline-block");
      div.html((d.label)+"<br>Score: "+(d.value));
      var iconUrl = 'http://maps.google.com/mapfiles/kml/pushpin/';
      saver[d.label].m.setIcon({
        url: iconUrl + 'red-pushpin.png',
        size: new google.maps.Size(32, 32),
        scaledSize: new google.maps.Size(32, 32)
      });
      saver[d.label].i.open(whomap, saver[d.label].m);
  });

  bar.on("mouseout", function(d){
    div.style("display", "none");
    var iconUrl = 'http://maps.google.com/mapfiles/kml/pushpin/';
    saver[d.label].m.setIcon({
      url: iconUrl + 'blue-pushpin.png',
      size: new google.maps.Size(32, 32),
      scaledSize: new google.maps.Size(32, 32)
    });
    saver[d.label].i.close();
  });

  svg.insert("g",":first-child")
      .attr("class", "axisHorizontal")
      .attr("transform", "translate(" + (margin + labelWidth) + ","+ (height - axisMargin - margin)+")")
      .call(xAxis);
  }

  var map;

  /*42.2820028,-71.0871498*/
  function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
      zoom: 12,
      center: new google.maps.LatLng(42.3223948,-71.0943483),
      mapTypeId: 'roadmap'
    });

    var iconUrl = 'http://maps.google.com/mapfiles/kml/pushpin/';
    var image = {
      url: iconUrl + 'blue-pushpin.png',
      size: new google.maps.Size(32, 32),
      scaledSize: new google.maps.Size(32, 32)
    };
    var imageH = {
      url: iconUrl + 'red-pushpin.png',
      size: new google.maps.Size(32, 32),
      scaledSize: new google.maps.Size(32, 32)
    };

    $.ajax({
        url: "http://datamechanics.io/data/rengx_ztwu_lwj/map.json",
        success: function (data) {
            var obj = JSON.parse(data);
            data = [];
            obj.forEach(function(s) {
              var x = s.x/(1.0*1000000);
              var y = s.y/(-1.0*1000000);
              var name = s.name;
              var addr = s.addr;
              var zipp = s.zipp;
              var access = s.access;

              var hos = document.getElementById("hos").value;
              var gar = document.getElementById("gar").value;
              var pol = document.getElementById("pol").value;
              var mar = document.getElementById("mar").value;

              var gs = s.access["garden"].length * gar;
              var hs = s.access["hospital"].length * hos;
              var ps = s.access["police"].length * pol;
              var ms = s.access["market"].length * mar;
              var score = gs + hs + ps + ms;

              var item = {
                label: s.name,
                value: score
              }
              data.push(item);

              var txt = name + "<br/>" + addr + "<br/>" + zipp + "<br/>Position: " + x + ", " + y +"<br/><b>Accessibility Score:" + score + "</b>";

              var marker = new google.maps.Marker({
                  position: new google.maps.LatLng(x, y),
                  icon: image,
                  map: map
                });
              var infowindow = new google.maps.InfoWindow({
                content: txt,
                extra: s.name
              });
              saver[s.name] = {
                m:marker,
                i:infowindow
              };

              google.maps.event.addListener(marker, 'mouseover', (function () {
                marker.setIcon(imageH);
                infowindow.open(map, marker);
                d3.selectAll('svg').remove();
                d3.select("#histo").remove();
                drawHist(data, infowindow.extra);
              }));
              google.maps.event.addListener(marker, 'mouseout', (function () {
                infowindow.close();
                marker.setIcon(image);
                d3.selectAll('svg').remove();
                d3.select("#histo").remove();
                drawHist(data, "");
              }));

            }); /*end of for each*/
            data.sort(function(a, b) {
              return parseFloat(b.value) - parseFloat(a.value);
            });
            data = data.slice(0,50);
            drawHist(data, "");
            whomap = map;
        }
    });

  }

  document.getElementById('sbtn').addEventListener('click', (function () {
    d3.selectAll('svg').remove();
    d3.select("#histo").remove();
    initMap();
  }));
