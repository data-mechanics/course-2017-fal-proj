var width = 960,
    height = 500,
    centered;

// Define color scale
var color = d3.scale.linear()
  .domain([1, 20])
  .clamp(true)
  .range(['#ffffb2', '#bd0026']);

var projection = d3.geo.mercator()
    .scale(100000)
    .rotate([71.057, 0])
    .center([0, 42.313]) 
    .translate([width / 2, height / 2]);

var path = d3.geo.path()
  .projection(projection);

// Set svg width & height
var svg = d3.select('svg')
  .attr('width', width)
  .attr('height', height);

// Add background
svg.append('rect')
  .attr('class', 'background')
  .attr('width', width)
  .attr('height', height)
  .on('click', clicked);

var g = svg.append('g');

var mapLayer = g.append('g')
  .classed('map-layer', true);

var BASE_FONT = "'Helvetica Neue', Helvetica, Arial, sans-serif";
var fontFamily = "Bitter" + ', ' + BASE_FONT;


var bigText = g.append('text')
  .classed('big-text', true)
  .attr('x', 20)
  .attr('y', 45);

var box = svg.append("rect")
    .attr("x", 15)
    .attr("y", 10)  
    .attr("fill", "#fff")
    .attr("stroke", "#000")
    .attr("width", 200)
    .attr("height", 180)
    .attr("opacity", 0);

var zipcode = svg.append('text')
    .attr('x', 20)
    .attr('y', 30)
    .attr('opacity', 1)

var percent_white = svg.append('text')
    .attr('x', 20)
    .attr('y', 45)
    .attr('opacity', 1)

var percent_black = svg.append('text')
    .attr('x', 20)
    .attr('y', 60)
    .attr('opacity', 1)

var percent_asian = svg.append('text')
    .attr('x', 20)
    .attr('y', 75)
    .attr('opacity', 1)

var percent_hispanic = svg.append('text')
    .attr('x', 20)
    .attr('y', 90)
    .attr('opacity', 1)

var percent_married = svg.append('text')
    .attr('x', 20)
    .attr('y', 105)
    .attr('opacity', 1)

var unemployment = svg.append('text')
    .attr('x', 20)
    .attr('y', 120)
    .attr('opacity', 1)

var median_income = svg.append('text')
    .attr('x', 20)
    .attr('y', 135)
    .attr('opacity', 1)

var median_rent = svg.append('text')
    .attr('x', 20)
    .attr('y', 150)
    .attr('opacity', 1)

var percent_poverty = svg.append('text')
    .attr('x', 20)
    .attr('y', 165)
    .attr('opacity', 1)

var percent_old = svg.append('text')
    .attr('x', 20)
    .attr('y', 180)
    .attr('opacity', 1)

// Load map data
d3.json('static/boston.geojson', function(error, mapData) {
    var features = mapData.features;
    // Update color scale domain based on data
    color.domain([0, d3.max(features, nameLength)]);

    // Draw each province as a path
    mapLayer.selectAll('path')
        .data(features)
        .enter().append('path')
        .attr('d', path)
        .attr('vector-effect', 'non-scaling-stroke')
        .style('fill', fillFn)
        .on('mouseover', mouseover)
        .on('mouseout', mouseout)
        .on('click', clicked);
});

// Get zipcode
function nameFn(d){
  return d && d.properties ? d.properties.ZIP5 : null;
}

// Get zipcode length
function nameLength(d){
  var n = nameFn(d);
  return n ? n.length : 0;
}

// Get zipcode color
function fillFn(d){
    var score = Math.floor(scores[d.properties.ZIP5]*5);
    if (isNaN(score)){
        score = -1;
    }
    return color(score);
}

// When clicked, zoom in
function clicked(d) {
    var x, y, k;

    // Compute centroid of the selected path
    if (d && centered !== d) {
        var centroid = path.centroid(d);
        x = centroid[0];
        y = centroid[1];
        k = 4;
        centered = d;
    } else {
        x = width / 2;
        y = height / 2;
        k = 1;
        centered = null;
    }

    // Highlight the clicked province
    mapLayer.selectAll('path')
        .style('fill', function(d){return centered && d===centered ? 'yellow' : fillFn(d);});

    // Zoom
    g.transition()
        .duration(750)
        .attr('transform', 'translate(' + width / 2 + ',' + height / 2 + ')scale(' + k + ')translate(' + -x + ',' + -y + ')');

    infoBox(d);
}

function mouseover(d){
    // Highlight hovered province
    d3.select(this).style('fill', 'yellow');

    // Draw effects
    textArt(nameFn(d));
}

function mouseout(d){
    // Reset province color
    mapLayer.selectAll('path')
        .style('fill', function(d){return centered && d===centered ? 'yellow' : fillFn(d);});

    // Clear zipcode
    bigText.text('');
}


function textArt(text){
  bigText
    .style('font-family', fontFamily)
    .text(text);
}

function infoBox(d) {
    if (centered && d===centered) {
        box.transition()
            .duration(750)
            .attr("opacity", 1);

        zipcode.transition()
            .duration(750)
            .attr("opacity", 1)
            .text(d.properties.ZIP5);

        percent_white.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent White: " + (zipinfo[d.properties.ZIP5]["percent_white"]*100).toFixed(1) + "%");

        percent_black.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent Black: " + (zipinfo[d.properties.ZIP5]["percent_black"]*100).toFixed(1) + "%");

        percent_asian.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent Asian: " + (zipinfo[d.properties.ZIP5]["percent_asian"]*100).toFixed(1) + "%");

        percent_hispanic.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent Hispanic: " + (zipinfo[d.properties.ZIP5]["percent_hispanic"]*100).toFixed(1) + "%");

        percent_married.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent Married: " + (zipinfo[d.properties.ZIP5]["percent_married_households"]*100).toFixed(1) + "%");
        
        unemployment.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Unemployment: " + (zipinfo[d.properties.ZIP5]["percent_unemployed"]*100).toFixed(1) + "%");
        
        median_income.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Median Income: $" + (zipinfo[d.properties.ZIP5]["median_income"]).toString() );
        
        median_rent.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Median Rent: $" + (zipinfo[d.properties.ZIP5]["median_rent"]).toString() );
        
        percent_poverty.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent Poverty: " + (zipinfo[d.properties.ZIP5]["percent_povery"]*100).toFixed(0) + "%");
        
        percent_old.transition()
            .duration(750)
            .attr("opacity", 1)
            .text("Percent of Old Buildings: " + (zipinfo[d.properties.ZIP5]["percent_homes_built_before_1939"]*100).toFixed(0) + "%");

            
    } else {
        box.transition()
            .duration(750)
            .attr("opacity", 0);

        zipcode.transition()
            .duration(750)
            .attr("opacity", 0)

        percent_white.transition()
            .duration(750)
            .attr("opacity", 0)

        percent_black.transition()
            .duration(750)
            .attr("opacity", 0)

        percent_asian.transition()
            .duration(750)
            .attr("opacity", 0)

        percent_hispanic.transition()
            .duration(750)
            .attr("opacity", 0)

        percent_married.transition()
            .duration(750)
            .attr("opacity", 0)
        
        unemployment.transition()
            .duration(750)
            .attr("opacity", 0)
        
        median_income.transition()
            .duration(750)
            .attr("opacity", 0)
        
        median_rent.transition()
            .duration(750)
            .attr("opacity", 0)
        
        percent_poverty.transition()
            .duration(750)
            .attr("opacity", 0)
        
        percent_old.transition()
            .duration(750)
            .attr("opacity", 0)


    }
}
