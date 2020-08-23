**1) Introduction**

This project undertakes the creation of a series of ETL transformations whose ultimate goal is the creation of a database that allows the run of Machine Learning algorithms that can unveil hidden patterns in immigration data.

Indeed, these studies want to analyze the impact of several ambiental and socioeconomic factors in the US immigrant population in relation to their incoming and distribution within the US.

We will start small, by gathering and cleaning the data, but only joining some econonmic data to the immigration tables, leaving the rest of the tables readily available at a one-join-statement distance, when the need of more columns presents.

At end, we will store the heavily modified Immigration table in a parquet file, so as it is possible to load it and use it without the need of running all these steps from scratch.

The project follows the follow steps:

* Step 1: Scope the Project and gather and present the Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up



**2) Data**

We'll be working with several files:

* SAS files: several files SAS files, containing the immigration data, from which one is used along the script: i94_apr16_sub.sas7bdat. The information is available at:  https://travel.trade.gov/research/reports/i94/historical/2016.html. Not stored in this repo because of weight reasons
* GlobalLandTemperaturesByCity.csv: from https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data. Not stored in this repo because of weight reasons
* immigration_data_sample.csv: a sample file from the SAS files, with 1000 rows, to get a feeling on the data
* airport-codes_csv.csv: airport code tables, from  https://datahub.io/core/airport-codes#data

* Countries_GDP_per_capita.csv: GDP per capita data, from the World Bank datasets: https://data.worldbank.org/indicator/NY.GDP.PCAP.CD?view=chart

* us-cities-demographics.csv: demographic data of several US cities. Comes from: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/



**3) Files in repo**

Inside this folder you may find:

***Auxiliary***

*country_codes.csv* : hand-made table with correspondence between several different codes for referring to countries

*Data dictionary.md* : our final schema data dictionary

*I94_SAS_Labels_Descriptions.SAS* : label descriptions for the Immigration SAS files

*sql_create_tables.sql* : queries used when creating table for creating the MySQL schema

*my_queries_sql.sql* : queries used when creating MySQL schema

***IMAGES***

A folder containing images used in the *Data Engineering Capstone Project.ipynb*

***out***

Folder for output parquet file

***treated***

Folder where some intermediate results are stored, in the form of csv files



***Data Engineering Capstone Project.ipynb*** --> ipython notebook with the code and reasoning



**4) Instructions**

1) The *Data Engineering Capstone Project.ipynb* file runs on some data that it is not stored in this project, because of weight reasons. 

Also, when downloaded, the parquet files and temperatures file are to be downloaded and correctly placed in their corresponding folders. Directions to those folders may be found within the ipython notebook (for instance: *./:./../data/* for all the parquet files and  *./:./../data2/* for the *GlobalLandTemperaturesByCity.csv*)

2) Open and run normally the *Data Engineering Capstone Project.ipynb* file. 

