# __Real-time stock market analysis using Kafka__

## __Introduction__
This project implements an End-To-End Data Engineering Project on Real-Time Stock Market Data using Kafka.

<div align="center">
  <img src="./ScreenShots/1_project_workflow.jpg" alt="workflow" width=647 height=364>
</div>

<br>

An __Apache Kafka Server__ is setup along with a __Apache Zookeeper__ fetch and monitor the dynamically rendering data.

<div align="center">
  <img src="./ScreenShots/18_connected_producer_consumer_in_action.jpg" alt="workflow" width=647 height=364>
</div>

<br>

The real-data production is simulated using __Python__. We can also setup a an API that produces real-time stock market data.

<div align="center">
  <img src="./ScreenShots/30_simulating_dynamic_data.jpg" alt="workflow" width=647 height=364>
</div>

<br>

The data is stored on the __AWS S3 Bucket__ and crawled using __AWS Glue Crawler__.

<div align="center">
  <img src="./ScreenShots/41_dynamic_data_generation.jpg" alt="workflow" width=647 height=364>
</div>

<br>


## __Technologies Used__
- __Python__
- __Amazon Web Service (AWS)__
  1. S3 (Simple Storage Service)
  2. Athena
  3. Glue Crawler
  4. Glue Catalog
  5. EC2
- __Apache Kafka__


## __Dataset__

We can use any dataset. The project mainly focuses on the operational side of Data Engineering (building data pipeline).

I have used a stored-static data saved as a [CSV File](./stock_market_data/indexProcessed.csv).

## __Implementation__

Follow the [ScreenShots](./ScreenShots/) for implementation.

The [commands.txt](./commands.txt) file provides all instructions to create the Kafka Server.
