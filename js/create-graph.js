var createGraph = function(query) {
  var width = 960,
      height = 500;

  var color = d3.scale.category20();

  var force = d3.layout.force()
      .charge(-120)
      .linkDistance(30)
      .size([width, height]);

  d3.select('svg')
       .remove();

  var svg = d3.select('#graph').append('svg')
      .attr('width', width)
      .attr('height', height);

  d3.json(searchUrl+query, function(error, data) {
    if (error) throw error;

    var graph = {};
    var centerNode = [{
      'id': data[0]['Data']['VertexId'],
      'name': data[0]['Data']['URL'],
      'rank': data[0]['Data']['PageRank'],
      'group': 1
    }];
    var neighbors = data[0]['Neighbors']['FirstDegree'];
    var neighborNodes = neighbors.slice(0,200).map(function(item, index) {
      var d = {};
      d['id'] = item;
      $.ajax({
        url: dataUrl+item,
        dataType: 'json',
        success:  function(data) {
          d['name'] = data['URL']
          d['rank'] = data['PageRank'];
        }
      });
      d['group'] = Math.floor((Math.random() * 10) + 1);
      return d;
    });
    graph.nodes = centerNode.concat(neighborNodes);

    graph.links = neighborNodes.map(function(item, index) {
      var d = {};
      d['source'] = index+1;
      d['target'] = 0;
      d['value'] = 1;
      return d;
    });

    // console.log(graph.links);

    force
        .nodes(graph.nodes)
        .links(graph.links)
        .start();

    var link = svg.selectAll(".link")
        .data(graph.links)
      .enter().append("line")
        .attr("class", "link")
        .style("stroke-width", function(d) { return Math.sqrt(d.value); });

    var node = svg.selectAll(".node")
        .data(graph.nodes)
      .enter().append("circle")
        .attr("class", "node")
        .attr("r", 5)
        .style("fill", function(d) { return color(d.group); })
        .call(force.drag);

    node.append("title")
        .text(function(d) { return d.name; });

    force.on("tick", function() {
      link.attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; })
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });

      node.attr("cx", function(d) { return d.x; })
          .attr("cy", function(d) { return d.y; });
    });
  });
}
