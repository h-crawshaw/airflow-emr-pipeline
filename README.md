# airflow-emr-pipeline
This repo demonstrates how Airflow can be used to create
a straightforward batch ingestion pipeline utilizing a few key AWS services. This was mostly done as an Airflow exercise. 

Using the following PUBG dataset (https://www.kaggle.com/datasets/skihikingkevin/pubg-match-deaths), I assume that the agg_match_stats CSV files are to be uploaded on a daily basis into an S3 bucket partitioned by a date key. Another assumption is that the volume of data is much larger than it actually is, otherwise EMR and Redshift may be over-engineering in this instance. The data is to be uploaded to HDFS and processed using a Spark cluster on EMR. It must be compressed and formatted in such a way that the subsequent Redshift COPY command will be most efficient. It will be loaded to a Redshift table and then potentially visualized using QuickSight.

# Overview
The image below gives a high-level overview of the pipeline:

![overviewimage drawio](https://user-images.githubusercontent.com/110303838/182579765-ab4915d0-47f7-4ff3-b248-7d2fdbd42533.png)

1) 

# To do:
Add Airflow to a k8s cluster
Add a Snowflake operator as an alternative to Redshift
