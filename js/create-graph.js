var createGraph = function(query) {
  var width = 960,
      height = 500;

  var color = d3.scale.category20();

  var force = d3.layout.force()
      .charge(-120)
      .linkDistance(function(link, index) {
        console.log(link);
        console.log((1/link['value'])*1500);
        return (1/link['value'])*1500;
      })
      .size([width, height]);

  d3.select('svg')
       .remove();

  var svg = d3.select('#graph').append('svg')
      .attr('width', width)
      .attr('height', height);

  d3.json(searchByUrl+query, function(error, data) {
    if (error) throw error;

    var graph = {};
    var centerNode = [{
      'id': data[0]['Data']['VertexId'],
      'name': data[0]['Data']['URL'],
      'rank': data[0]['Data']['PageRank'],
      'group': 1
    }];
    var neighbors = data[0]['Neighbors']['FirstDegree'];
    var neighborNodes = neighbors.slice(0,100).map(function(item, index) {
      var d = {};
      $.ajax({
        url: dataById+item,
        dataType: 'json',
        success:  function(data) {
      d['id'] = item;
      d['group'] = Math.floor((Math.random() * 10) + 1)+1;
          d['name'] = data['URL'];
          d['rank'] = data['PageRank'];
        },
        async: false
      });
      return d;
    });
    graph.nodes = centerNode.concat(neighborNodes);

    graph.links = neighborNodes.map(function(item, index) {
      var d = {};
      d['source'] = index+1;
      d['target'] = 0;
      d['value'] = item['rank'];
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
        .attr("r", 10)
        .style("fill", function(d) { return color(d.group); })
        .call(force.drag);

    var center = svg.select(".node").attr("r", 20);

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
