var houseicon = {};
var fancymap;
var map;


function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
      zoom: 12,
      /* Center at Fenway Park 42.3467,-71.0972*/
      center: new google.maps.LatLng(42.3467,-71.0972),
      mapTypeId: 'roadmap'
    });

    var iconUrl = 'https://maps.google.com/mapfiles/kml/pal2/';
    var image = {
      url: iconUrl + 'icon10.png',
      size: new google.maps.Size(32, 32),
      scaledSize: new google.maps.Size(32, 32)
    };
    var imageH = {
      url: iconUrl + 'icon2.png',
      size: new google.maps.Size(32, 32),
      scaledSize: new google.maps.Size(32, 32)
    };


    $.ajax({
        url: "http://datamechanics.io/data/eileenli_xtq_yidingou/schoolscore.json",
        success: function (data) {
            var obj = JSON.parse(data);
            data = [];
            obj.forEach(function(s) {
              var x = s.coordinates[1].toFixed(6);
              var y = s.coordinates[0].toFixed(6);
              var school = s.school;
              var safety = Math.floor(s.safety);
              var comfort = Math.floor(s.comfort);
              var traffic = Math.floor(s.traffic);

              var saf = document.getElementById("saf").value;
              var tra = document.getElementById("tra").value;
              var com = document.getElementById("com").value;

              // calculating the scores
              var ss = safety * saf;
              var tt = traffic * tra;
              var cc = comfort * com;
              var score = Math.floor(ss + tt + cc);

              var item = {
                label: s.school,
                value: score
              }
              data.push(item);

              var txt = school + "<br/> safety: " + safety + "<br/> comfort: " + comfort + "<br/> traffic: " + traffic + "<br/>Position: " + x + ", " + y +"<br/><b>Total Score:" + score + "</b>";

              // maker declaration
              var marker = new google.maps.Marker({
                  position: new google.maps.LatLng(x, y),
                  icon: image,
                  map: map
                });

              // the corresponding information will appear on top of icon selected
              var infowindow = new google.maps.InfoWindow({
                content: txt,
                extra: s.school
              });
              houseicon[s.school] = {
                m:marker,
                i:infowindow
              };

              google.maps.event.addListener(marker, 'mouseover', (function () {
                marker.setIcon(imageH);
                infowindow.open(map, marker);
                d3.selectAll('svg').remove();
                d3.select("#histo").remove();
                histogram(data, infowindow.extra);
              }));
              google.maps.event.addListener(marker, 'mouseout', (function () {
                infowindow.close();
                marker.setIcon(image);
                d3.selectAll('svg').remove();
                d3.select("#histo").remove();
                histogram(data, "");
              }));

            }); 
            data.sort(function(a, b) {
              return parseFloat(b.value) - parseFloat(a.value);
            });
            data = data.slice(0,60);
            histogram(data, "");
            fancymap = map;
        }
    });

  }
function histogram(data,special){
  var div = d3.select("body")
              .append("div")
              .attr("class", "toolTip")
              .attr("id","histo");

  var axisMargin = 30,
      margin = 10,
      valueMargin = 4,
      width = 620,
      height = 1000,
      barHeight = 450/data.length,
      barPadding = 450/data.length,
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
     .attr("dy", ".35em") 
     .attr('fill', 'white')
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

  // when user move their mouse on or off the icon on the map, corresponding school will be selected
  bar.append("rect")
      .attr("transform", "translate("+labelWidth+", 0)")
      .attr("height", barHeight)
      .attr("fill", function(d) {
        if(special == ""){
          return "#e5b6ed";
        }
        else{
          if (d.label == special) {
            return "#bfefc9";
          }
          else {
            return "#e5b6ed";
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
      .style("font-size", "24px")
      .attr('fill', 'white')
      .text("Top 60 Colleges");

  // When user move mouse on or off names of school, same bar will be selected.
  svg.selectAll("rect")
      .on("mouseover", function(d){
        d3.select(this).attr("fill","#bfefc9");
      })
      .on("mouseout", function(d){
        d3.select(this).attr("fill","#e5b6ed");
      })

  // When user move mouse on or off the bar, corresponding school will be selected on map  
  bar.on("mouseover", function(d){
      div.style("left", d3.event.pageX+10+"px");
      div.style("top", d3.event.pageY-25+"px");
      div.style("display", "inline-block");
      div.html((d.label)+"<br>Score: "+(d.value));
      var iconUrl = 'https://maps.google.com/mapfiles/kml/pal2/';
      houseicon[d.label].m.setIcon({
        url: iconUrl + 'icon2.png',
        size: new google.maps.Size(32, 32),
        scaledSize: new google.maps.Size(32, 32)
      });
      houseicon[d.label].i.open(fancymap, houseicon[d.label].m);
  });

  bar.on("mouseout", function(d){
    div.style("display", "none");
    var iconUrl = 'https://maps.google.com/mapfiles/kml/pal2/';
    houseicon[d.label].m.setIcon({
      url: iconUrl + 'icon10.png',
      size: new google.maps.Size(32, 32),
      scaledSize: new google.maps.Size(32, 32)
    });
    houseicon[d.label].i.close();
  });

  svg.insert("g",":first-child")
      .attr("class", "axisHorizontal")
      .attr("transform", "translate(" + (margin + labelWidth) + ","+ (height - axisMargin - margin)+")")
      .call(xAxis);
  }

document.getElementById('sbtn').addEventListener('click', (function () {
    d3.selectAll('svg').remove();
    d3.select("#histo").remove();
    initMap();
  }));
