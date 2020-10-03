# timeseries-covid

AWS recently launched [AWS Timestream service](https://aws.amazon.com/blogs/aws/store-and-access-time-series-data-at-any-scale-with-amazon-timestream-now-generally-available/). 

This project consists of an ingestion python script to ingest [COVID-19 data](https://www.kaggle.com/lihyalan/2020-corona-virus-timeseries) to the AWS managed time series database for learning and analysis purpose.  

Important concepts:

Time series: Sequenece of data points records over a time interval
Record: A single data point containing a time
Dimension: Describes the meta data of a timeseries. A dimension consist of dimension name and dimension value. 
For e.g. 
    - Considering a dimension name for a weather forecasting agency can be "ECMWF"
    - Considering an AWS region as dimension, then can value can be "eu-west-1"
    - For an IoT device, the dimension mame can be "Sensor ID" and value can be "12345"

Setup:

pip3 install boto3
