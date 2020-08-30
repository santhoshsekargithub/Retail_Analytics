# Retail_Analytics project
    Building of this Data lake project provides an all in one central repository for converged Data Platformthat helps retailers chain of stores to integrate and analyze a wide variety of online and offline customer data including the customer data, products, purchases, orders, employees and comments data. Retailers can analyze this data to generate insights about individual consumer behaviors and preferences,Recommendations and Loyalty Programs. This tool builds the strategies, Key Performance Indicators (KPI) definitions and implementation roadmaps that assists our esteemed clients in their Analytics & Information ecosystem journey right from Strategy definition to large scale Global Implementations & Support using the BigData ecosystems.
#Project Flow:
![alt text](https://github.com/santhoshsekargithub/Retail_Analytics/blob/master/Architecture.PNG)
1. Import the data from MYSQL DB using spark jdbc option
2. Persist the processed data into HDFS
3. Spark SQL to process the data and load to HDFS
4. Load the processed data to Hive external table
5. Load the final converted data to Elastic Search.
6. Create Visualization and dashboard using Kibana.

![alt text](https://github.com/santhoshsekargithub/Retail_Analytics/blob/master/Source_schema.PNG)

#Prerequisites:
Ensure – Hadoop, Spark, Hive metastore, Elastic Search and Kibana are running.
Run the below sqls to create and load data in the tables created in the below schemas
ordersproduct_ORIG.sql - Schema: ordersproducts, Tables: orders, products, orderdetails custpayments_ORIG.sql - Schema: custpayments, Tables: custpayments and paymentspayments empoffice.sql - Schema: empoffice, Tables: employees and offices
mysql>
source /home/hduser/retailordersspark/ordersproduct_ORIG.sql
source /home/hduser/retailordersspark/custpayments_ORIG.sql
source /home/hduser/retailordersspark/empoffice.sql

hive –service metastore
hive
create database retail_mart;
create external table IF NOT EXISTS retail_mart.custordfinal (customernumber STRING, customername STRING, contactfullname string, addressLine1 string,city string,state string,country string,phone bigint,creditlimit float,checknum string,checkamt int,ordernumber STRING,shippeddate date,status string, comments string,productcode string,quantityordered int,priceeach double,orderlinenumber int,productName STRING,productLine STRING,productScale STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice double,MSRP double,orderdate date) stored as orcfile location 'hdfs://localhost:54310/user/hive/warehouse/retail_mart.db/custordfinal/';

#Job Submission: 
spark-submit --jars file:/home/hduser/install/mysql-connector-java.jar,file:/home/hduser/elasticsearch-hadoop-7.2.1.jar retailanalytics.py

