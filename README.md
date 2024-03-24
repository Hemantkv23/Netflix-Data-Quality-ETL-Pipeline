# Netflix Data Quality ETL Pipeline
S3, Glue, Redshift, CloudWatch, EventBridge and SNS

## Introduction
In this project, I have created an End-To-End Data Engineering Project on Netflix's movies/shows.

## Overview
I have created an ETL pipeline designed to extract data from the source S3, transform it to ensure data quality and consistency using AWS Glue, and load it into the Redshift for further analysis and reporting.

# Architecture
![Architecture](https://github.com/Hemantkv23/Netflix-movie-show-analysis/assets/43467503/d345da5f-3643-4bc8-b1bf-652df1b99fbd)

## Features
* Data Extraction: Retrieve metadata of Netflix movies and shows from the source S3 using Glue Crawler.
* Data Transformation: Apply quality checks, cleaning, and standardization to ensure high-quality data.
* Data Loading: Load transformed data into the destination Redshift table for analysis. And Loading Bad Data into S3 bucket for further analysis.
* Monitoring and Logging: Monitor the ETL pipeline's performance and log any errors or anomalies for easy troubleshooting and getting alerts on the gmail.

## Technology Used
* Amazon Web Services
1. S3 (Simple Storage Service)
2. Glue Crawler
3. Glue Catalog
4. Visual ETL
5. Redshift
6. CloudWatch
7. EventBridge
8. SNS (Social Networking Service)

## DataSet Used
Here's the DataSet link - https://www.kaggle.com/datasets/thedevastator/netflix-imdb-scores
