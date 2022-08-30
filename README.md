# Project Summary
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data 
warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as
a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them
using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to
continue finding insights in what songs their users are listening to.

You will be able to test your database and ETL pipeline by running queries given to you by the analytics team from 
Sparkify and compare your results with their expected results.

## Description
The project includes an ETL pipeline using Spark with Pyspark API to extract data from S3 bucket
`s3://udacity-dend/song-data` and `s3://udacity-dend/song-data` and load them to `s3://xzbits-sparkify-data-lake/`.
By deploying AWS EMR service to run ETL pipeline and building data lake on `s3://xzbits-sparkify-data-lake/`

## Database design in STAR schema
![Sparkify_star_schema.PNG](project3_star_schema.png "sparkifydb STAR schema")

## How to run Python file
# Step 1: Create AWS EMR cluster with the reference configuration as below:
* Release: `emr-5.20.0` or later
* Applications: `Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0`
* Instance type: `m3.xlarge`
* Number of instance: `3`

# Step 2: Connect to your EMR clusters via PuTTY or AWS console

# Step 3: Clone the Python scripts and clarify S3 Bucket
```commandline
git clone https://<YOUR_PERSONAL_ACCESS_TOKEN>>@github.com/xzbits/project4_Data_Lake_DE_ND.git
```

# Step 4: S3 Bucket
```buildoutcfg
[default]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
S3_BUCKET=
```
# Step 5: Run ETL pipeline
```commandline
python etl.py
```