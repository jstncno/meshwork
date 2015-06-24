var VertexListContainer = React.createClass({
  getInitialState: function() {
    return {data: []};
  },
  componentDidMount: function() {
    $.ajax({
      url: this.props.url,
      dataType: 'json',
      cache: false,
      success: function(data) {
        this.setState({data: data});
      }.bind(this),
      error: function(xhr, status, err) {
        console.error(this.props.url, status, err.toString());
      }.bind(this)
    });
  },
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
        <Vertex key={vertex.pageRank} url={vertex.url}>
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
  componentDidUpdate: function(prevProps, prevState) {
    var $vertexList = $('.testWrapper');
    $wrapper.find('.vertex').sort(function (a, b) {
      return -a.getAttribute('key') - -b.getAttribute('key');
    }).appendTo( $wrapper );    
  },
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
  <VertexListContainer url="js/vertices.json" />,
  document.getElementById('content')
);
