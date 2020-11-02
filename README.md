# Sparkify Pipeline

Sparkify Pipeline is a Redshift database that stores the user activity and songs metadata on Sparkify's new streaming application. Previously this information resided in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline is built that extracts their data from S3, stages them in Redshift, transforms data into a set of dimensional tables and fact table, performs data quality checks using airflow. Also using airflow UI, DAG tasks can be monitored during execution.

## Purpose

- The datawarehouse provides Sparkify's analytical team with access to their processes and data on cloud.
- The design of the database tables is optimized for queries on song play analysis.
- Use of airflow aloows us schedule the DAG runs according to requirement.
- Also, airflow UI allows easy monitoring of DAG task execution providing access to task status during execution as well as logs enabling easy debugging. 
- Airflow also allows to add a task to perform data quality checks to ensure quality of data loaded into datawarehouse tables.

## Database Schema Design

Sparkify datawarehouse tables form a star schema. This database design separates facts and dimensions yielding a subject-oriented design where data is stored according to logical relationships, not according to how the data was entered. 
- Fact And Dimension Tables

    The database includes:
    - Fact table:
        
        1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
            - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
            
    - Dimension tables:
        
        2. **users** - users in the app
            - user_id, first_name, last_name, gender, level
        3. **songs** - songs in music database
            - song_id, title, artist_id, year, duration
        4. **artists** - artists in music database
            - artist_id, name, location, latitude, longitude
        5. **time** - timestamps of records in songplays broken down into specific units
            - start_time, hour, day, week, month, year, weekday

## ETL Pipeline

An ETL pipeline is built using airflow and python. The airflow python script loads data from S3 to staging tables on Redshift. It also inserts data from staging tables into fact and dimensional tables as well as performs data quality checks on data loaded from S3 to datawarehouse tables.

## Project structure
    airflow 
    |---------- dags
                |---------- etl_dag.py
    |---------- plugins
                |---------- __init__.py
                |---------- helpers
                            |---------- __init__.py 
                            |---------- create_tables.sql 
                            |---------- sql_queries.py
                            |---------- dq_tests.py
                |---------- operators
                            |---------- __init__.py 
                            |---------- data_quality.py
                            |---------- load_dimension.py
                            |---------- load_fact.py
                            |---------- stage_redshift.py
    |---------- README.md
    |---------- dwh.cfg
    |---------- setup.py
    |---------- teardown.py
    
## Files in project structure
1. **setup.py**
This is a script to perform setup actions such as creating IAM role for Redshift cluster, attaching policy to the role, creating Redshift cluster, and allowing ingress of incoming traffic to cluster's default security group.

2. **etl_dag.py**
This is a script that defines tasks for a dag required to create staging and analytics tables on redshift, load data into these tables and perform data quality checks.

3. **create_tables.sql**
This is a sql file with queries to drop existing sparkify tables on redshift and create new staging and analytics tables.

4. **stage_redshift.py**
This is a script to copy data AWS S3 location to a staging table on AWS redshift.

5. **sql_queries.py**
This is a sql file with queries to select records to be inserted into sparkify analytics tables on redshift.

6. **load_dimension.py**
This is a script to load data into a dimension table in either append-only mode or delete-load mode on redshift.

7. **load_fact.py**
This is a script to load data into a fact table on redshift.

8. **dq_tests.py**
This is a script containing a dictionary of tables and expected record counts in them to test the quality of data in staging, dimension, or fact tables.

9. **data_quality.py**
This is a script to check quality of data loaded into a table based on information available in *dq_tests.py* script.

10. **teardown.py**
This is a script to perform teardown actions such as deleting IAM role for Redshift cluster, detaching policy to the role, deleting Redshift cluster, and revoking ingress of incoming traffic to cluster's default security group. 

11. **dwh.cfg**
This is a configuration file.

## Running Python Scripts 
The following python scripts are used to setup Redshift cluster, create staging and analytics tables on Redshift and to load data from S3 to staging tables using COPY queries and to insert the records from staging tables into the database tables:

1. **setup.py** 
This script performs setup actions such as creating IAM role for Redshift cluster, attaching policy to the role, creating Redshift cluster, and allowing ingress of incoming traffic to cluster's default security group. Run this file to setup Redshift cluster with required permissions before running create_tables script.
*Run below command in terminal to execute this script:*
    `python setup.py`

2. Login to your AWS account from AWS Console management. Navigate to IAM service and create an IAM admin user. An access key and secret access will be associated with this admin user.

3. Run below command in the terminal to start Airflow UI
    `/opt/airflow/start.sh`

4. In Airflow UI, Admin tab, create an AWS connection by entering 'aws_conn' as Conn Id, access key as Login and secret access as Password and clicking save.

5. Similarly, create a Postgres connection to refer to a specific AWS redshift cluster by entering 'redshift_conn' as Conn Id, Postgres as Conn Type, host, redshift database name as schema, database username as login, database password as password, 5439 as port and clicking save.

5. In Airflow UI, DAG tab, toggle the dag on for the scheduler to trigger the dag run.

6. Monitor the dag and its tasks for status and logs.

7. **teardown.py**
This script can be used for teardown actions such as deleting IAM role for Redshift cluster, detaching policy to the role, deleting Redshift cluster, and revoking ingress of incoming traffic to cluster's default security group. 
*Run below command in terminal to execute this script:*
        `python teardown.py`
