var VertexListContainer = React.createClass({
  getInitialState: function() {
    return {data: []};
  },
  componentWillReceiveProps: function(nextProps) {
    $.ajax({
      url: nextProps.url,
      dataType: 'json',
      cache: false,
      success: function(data) {
        this.setState({data: data});
      }.bind(this),
      error: function(xhr, status, err) {
        $('#throbber-loader-container').hide();
        console.error(this.props.url, status, err.toString());
      }.bind(this)
    });
  },
  render: function() {
    return (
      <div className="vertexListContainer">
        <VertexList data={this.state.data[0]} />
      </div>
    );
  }
});

var VertexList = React.createClass({
  getInitialState: function() {
    return {neighbors: [2]};
  },
  shouldComponentUpdate: function(nextProps, nextState) {
    // return nextProps.id !== this.props.id;
    // console.log(nextProps.data['Neighbors']['FirstDegree']);
    // console.log(this.state.neighbors);
    if(nextProps.data) {
        // hide spinner
        $('#throbber-loader-container').hide()
      // console.log(nextProps.data['Neighbors']['FirstDegree'] !== this.state.neighbors);
      return nextProps.data['Neighbors']['FirstDegree'] !== this.state.neighbors;
    }
    return true;
  },
  componentWillReceiveProps: function(nextProps) {
    var n = nextProps.data;
    if(n != undefined) {
      if(n['Neighbors']['FirstDegree']) {
        // console.log(n['Neighbors']['FirstDegree'].length + ' neighbors');
        var neighbors = n['Neighbors']['FirstDegree'];
        if(neighbors) this.setState({neighbors: neighbors.slice(0,100)});
        // else this.setState({neighbors: [0]});
      } else this.setState({neighbors: [0]});
    }
  },
  componentDidUpdate: function(nextProps, nextState) {
    if(this.state.neighbors.length == 0) {
        // hide spinner
        $('#throbber-loader-container').hide()
    }
  },
  render: function() {
    var vertexNodes = this.state.neighbors.map(function (vertex) {
      if(vertex===0) {
        return (
          <h2 className='notFound'>No neighbors found :(</h2>
        );
      }
      return (
        <Vertex key={vertex} vertexId={vertex}>
            {vertex}
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
  getInitialState: function() {
    return {data: []};
  },
  componentDidMount: function() {
    $.ajax({
      url: dataUrl+this.props.vertexId,
      dataType: 'json',
      cache: false,
      success: function(data) {
        this.setState({data: data});
        // hide spinner
        $('#throbber-loader-container').hide()
      }.bind(this),
      error: function(xhr, status, err) {
        console.error(this.props.url, status, err.toString());
      }.bind(this)
    });   
  },
  /*componentDidUpdate: function(nextProps, nextState) {
    var $vertexList = $('.vertexList');
    $vertexList.find('.vertex').sort(function (a, b) {
      console.log();
      var aVal = $($(a).find('.vertexPageRank')[0]).text();
      var bVal = $($(b).find('.vertexPageRank')[0]).text();
      return -aVal - -bVal;
    }).appendTo( $vertexList ); 
  },*/
  render: function() {
    return (
      <div className='vertex'>
        <h2 className='vertexUrl'>
          {this.state.data['URL']}
        </h2>
        <span className='vertexPageRank'>
          {this.state.data['PageRank']}
        </span>
      </div>
    );
  }
});

var Meshwork = React.createClass({
  getInitialState: function() {
    return {query: '', text: ''};
  },
  componentDidMount: function() {
    $('#throbber-loader-container').hide()
  },
  onChange: function(e) {
    this.setState({text: e.target.value});
  },
  handleSubmit: function(e) {
    e.preventDefault();
    var input = this.state.text;
    this.setState({query: searchUrl+input});
    // show spinner
    $('#throbber-loader-container').show()
    console.log(input);
    createGraph(input);
  },
  render: function() {
    return (
      <div>
        <div className='jumbotron'>
          <div className='container'>
            <h1>Hello, world!</h1>
            <p>This is a template for a simple marketing or informational website. It includes a large callout called a jumbotron and three supporting pieces of content. Use it as a starting point to create something more unique.</p>
            <p><a className="btn btn-primary btn-lg" href="#" role="button">Learn more &raquo;</a></p>
            <div className='input-container'>
              <h3>Meshwork</h3>
              <form onSubmit={this.handleSubmit} id='input-form'>
                <input onChange={this.onChange} value={this.state.text} />
              </form>
            </div>
          </div>
        </div>
        <div className='container'>
          <div id='meshwork-content-container'>
            <div id='throbber-loader-container'>
              <div className='throbber-loader'></div>
            </div>
            <VertexListContainer url={this.state.query} />
          </div>
        </div>
      </div>
    );
  }
});

React.render(
  <Meshwork />,
  document.getElementById('content')
);

