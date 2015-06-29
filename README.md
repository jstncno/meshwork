Meshwork
=======
Mesh + Network = Meshwork

[Meshwork](http://jcano.me/meshwork) is my big data project as part of [Insight Data Science](http://insightdataengineering.com/)'s Engineering fellowship program from June 2015 through July 2015.

# What is Meshwork?
## Abstract
If you own a website, then you own a piece of the vast internet web graph. When you look at your website's relationships (links) to other websites, then those relationships can be viewed, as what I like to call, a mesh in the internet network. So, Mesh + Network = Meshwork.

## Intro
*Meshwork* is an open-source data pipeline which extracts and processes [Common Crawl](http://commoncrawl.org)'s web corpus, finding the [Page Rank](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf) and 1<sup>st</sup> and 2<sup>nd</sup> degree relationships of each web page in the hyperlink graph. To do this, it leverages the following technologies:
* Big Data Pipeline
  * AWS EC2 and S3
  * Apache Hadoop
  * Apache Spark and Spark GraphX
  * Apache HBase
* Front End
  * Node.js
  * jQuery
  * React
