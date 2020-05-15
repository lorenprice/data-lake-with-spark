# DATA LAKE ON AWS
___
### OVERVIEW 
These files include the schema and ETL steps necessary to build a data lake for a music streaming service. The data lake contains songplay log information, as well as user, song, and artist information. The database can be queried to report on business performance or deliver user and product insights.  

The data lake is built on AWS using Spark and Elastic Map Reduce. The ETL extracts the play logs and song data from s3 buckets, creates and outputs tables as parquet files in a seperate s3 bucket.  
___
### SCHEMA
The final OLAP database is modeled in a start schema with 5 tables.  
* songplays - **fact table**
    - songplay_id
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent
* users
    - user_id
    - first_name 
    - last_name 
    - gender
    - level
* songs
    - song_id
    - title
    - artist_id 
    - year 
    - duration
* artists
    - artist_id 
    - name
    - location
    - latitude 
    - longitude
* time
    - start_time 
    - hour
    - day 
    - week 
    - month 
    - year 
    - weekday  
___
### SETUP
To initialize the pipeline and create the final parquet files, run **etl.py** to perform all of the ETL steps.
##### You will need a configuration file named ```dl.cfg``` with the following:
```
[AWS_KEYS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

```  