var VertexListContainer = React.createClass({
  /*getInitialState: function() {
    return {data: []};
  },*/
  render: function() {
    return (
      <div className="vertexListContainer">
        <VertexList data={this.state.data} />
      </div>
    );
  }
});

var VertexList = React.createClass({
  render: function() {
    var vertexNodes = this.props.data.map(function (vertex) {
      return (
        <Vertex url={vertex.url}>
          {vertex.pageRank}
        </Vertex>
      );
    });
    return (
      <div className="vertexList">
        {vertexNodes}
      </div>
    );
  }
});

var Vertex = React.createClass({
  render: function() {
    return (
      <div className="vertex">
        <h2 className="vertexUrl">
          {this.props.url}
        </h2>
        {this.props.children}
      </div>
    );
  }
});

React.render(
  <VertexListContainer data="vertices.json" />,
  document.getElementById('content')
);
