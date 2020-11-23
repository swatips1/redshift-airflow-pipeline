# ETL using Redshit and Airflow

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. 
The data is sourced from external source via means of a S3 bucket. The data - logs that tell about user activity in the application and  metadata about the songs the users listen to- are stored in for of JSON files. 

## Getting started


### Project structure explanation
* README.md: Description and other project information.
* Graph.png: Graph representation of the DAG  
* Tree.png: Tree representation of the DAG
* dags/etl.py: Main DAG code
* plugins/helpers/sql_queries: SQL Queries for table creation and population
* operators/data_quality.py: Code to check the quality of data loaded into Redfshit.
* operators/load_dimension.py: Code required for the S3 to Redshift data move
* operators/load_fact.py: Code required for the S3 to Redshift data move
* operators/stage_redshift.py: Code required for the S3 to Redshift data move

#### To Run

#### Create a Redsfhit Cluster in the location where the S3 bucket resides
* Make sure to enable public access to the cluster
* Make sure the port 5429 is property setup and is accessible for the role/group/cluster combination


#### Connect Airflow to AWS

1. Click on the Admin tab and select Connections.

2. Under Connections, select Create.

3. On the create connection page, enter the following values:
- Conn Id: Enter aws_credentials.
- Conn Type: Enter Amazon Web Services.
- Login: Enter your Access key ID from the IAM User credentials.
- Password: Enter your Secret access key from the IAM User credentials.
![aws_credentials](https://video.udacity-data.com/topher/2019/February/5c5aaefe_connection-aws-credentials/connection-aws-credentials.png)
Once you've entered these values, select Save and Add Another.

4. On the next create connection page, enter the following values:
- Conn Id: Enter redshift.
- Conn Type: Enter Postgres.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
- Schema: Enter dev. This is the Redshift database you want to connect to.
- Login: Enter awsuser.
- Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter 5439.
![redshift](https://video.udacity-data.com/topher/2019/February/5c5aaf07_connection-redshift/connection-redshift.png)
Once you've entered these values, select Save.

#### Start the DAG
Start the DAG by switching it state from OFF to ON.

Refresh the page and click on the s3_to_redshift_dag to view the current state.


