# Retail_Analytics project
    Building of this Data lake project provides an all in one central repository for converged Data Platformthat helps retailers chain of stores to integrate and analyze a wide variety of online and offline customer data including the customer data, products, purchases, orders, employees and comments data. Retailers can analyze this data to generate insights about individual consumer behaviors and preferences,Recommendations and Loyalty Programs. This tool builds the strategies, Key Performance Indicators (KPI) definitions and implementation roadmaps that assists our esteemed clients in their Analytics & Information ecosystem journey right from Strategy definition to large scale Global Implementations & Support using the BigData ecosystems.
#Project Flow:
1. Import the data from MYSQL DB using spark jdbc option
2. Persist the processed data into HDFS
3. Spark SQL to process the data and load to HDFS
4. Load the processed data to Hive external table
5. Load the final converted data to Elastic Search.
6. Create Visualization and dashboard using Kibana.

#Prerequisites:
Ensure – Hadoop, Spark, Hive metastore, Elastic Search and Kibana are running.
Run the below sqls to create and load data in the tables created in the below schemas
ordersproduct_ORIG.sql - Schema: ordersproducts, Tables: orders, products, orderdetails custpayments_ORIG.sql - Schema: custpayments, Tables: custpayments and paymentspayments empoffice.sql - Schema: empoffice, Tables: employees and offices
mysql>
source /home/hduser/retailordersspark/ordersproduct_ORIG.sql
source /home/hduser/retailordersspark/custpayments_ORIG.sql
source /home/hduser/retailordersspark/empoffice.sql
