from __future__ import print_function
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as fn

#spark=SparkSession.builder.appName("Hackathon").enableHiveSupport().getOrCreate()

def main():
    spark =SparkSession.builder.appName('RetailAnalytics-Hive').enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
	#calling method to pull offices table data from mysql db under
    employee = getRdbmsData(spark,"empoffice","employees","employeeNumber")
    print("employee :Employee data successfully pulled from Mysql!!!" )
    employee.show(5)

    #offices :calling method to pull offices table data from mysql db under empoffice schema
    offices = getRdbmsData(spark,"empoffice","offices","officeCode")
    print("offices : Offices data successfully pulled from Mysql!!!")	
    offices.show(5)

    #calling method to pull data from mysql db under custpayments schema 
    db3_cust = getRdbmsData(spark, "custpayments", "customers", "city")
    db3_pay = getRdbmsData(spark, "custpayments", "payments", "paymentDate")
    db3_join = db3_cust.join(db3_pay, 'customerNumber')
    cust_payment=db3_join.select("customerNumber",fn.upper(db3_join.customerName).alias("customerName"),"contactFirstName","contactLastName","phone","addressLine1","city","state","postalCode","country","salesRepEmployeeNumber","creditLimit","checknumber","paymentdate","amount")
    print("cust_payment : Customer and payment joined data successfully pulled from Mysql!!!")
    cust_payment.show(5)	
	
    #calling method to pull data from mysql db under ordersproducts schema using custom sql
    db4_order = getRdbmsData(spark, "ordersproducts", "orders", "orderdate")
    db4_orddet= getRdbmsData(spark,"ordersproducts","orderdetails","productCode")
    db4_prod= getRdbmsData(spark,"ordersproducts","products","productCode")
    db4_join1=db4_order.join(db4_orddet,'orderNumber')
    db4_join2=db4_join1.join(db4_prod,'productCode')
    order_products=db4_join2.drop('requiredDate')
    print("order_products :Order data successfully pulled from Mysql!!!")
    order_products.show(5)
	
    #Calling method to process customer data
    processCustPayData(spark,employee,offices,cust_payment,order_products)
    print("Customer data has been processed successfully and loaded into retail_mart.custordfinal_new Hive table")
    print("Cutomer payment details availabe in /user/hduser/output/CustPayment file")
    print("Order product details available in /user/hduser/output/order_products file ")

def writetoes(df):
    df.write.mode("overwrite").format("org.elasticsearch.spark.sql").option("es.resource", "custfinales2/doc1").save()	
    print("ES write")

def getRdbmsData(spark,DatabaseName, TableName, PartitionColumn):
    Driver = "com.mysql.jdbc.Driver"
    Host = "localhost"
    Port = "3306"
    User = "root"
    Pass = "root"
    url = "jdbc:mysql://{0}:{1}/{2}".format(Host, Port, DatabaseName)
    df_db = spark.read.format("jdbc") \
    .option("driver", Driver) \
    .option("url", url) \
    .option("user", User) \
    .option("lowerBound", 1) \
    .option("upperBound", 10000) \
    .option("numPartitions", 4) \
    .option("partitionColumn", PartitionColumn) \
    .option("password", Pass) \
    .option("dbtable", TableName).load()
    return df_db

def processCustPayData(spark,employee,offices,cust_payment,order_products):
    #processing customer details and registering as custdetcomplextypesp
    custdetcomplextypes=cust_payment.select("customerNumber", "customerName", fn.concat(fn.col("contactFirstName"),fn.lit(" "),fn.col("contactLastName")).alias("contactfullname"), "phone", "addressLine1", "city", "state", "postalCode", "country","salesRepEmployeeNumber", "creditLimit", "checknumber", "paymentdate", "amount")
    #custdetcomplextypes.show(5)
    custpaymentcomplextypes=cust_payment.select(fn.concat("customerNumber",fn.lit("~"),"checknumber",fn.lit("~"),fn.concat("creditLimit",fn.lit("$"),"amount"),fn.lit("~"),"paymentDate"))
    custpaymentcomplextypes.show(5,False)
    custpaymentcomplextypes.coalesce(1).write.mode("overwrite").option("header","true").csv("hdfs://localhost:54310/user/hduser/output/CustPayment")
	
    #processing order details and registering as orddetcomplextypes
    orddetcomplextypes=order_products.select("customerNumber","ordernumber","shippeddate","status","comments","productcode","quantityordered","priceeach","orderlinenumber","productName","productLine","productScale","productVendor","productDescription","quantityInStock","buyPrice","MSRP","orderdate")
    orddetcomplextypes.coalesce(1).write.mode("overwrite").option("header","true").csv("hdfs://localhost:54310/user/hduser/output/order_products")
    spark.sql("""create database if not exists retail_mart""")
    spark.sql("""create external table IF NOT EXISTS retail_mart.custordfinal_new (customernumber STRING, customername
    STRING, contactfullname string, addressLine1 string,city string,state string,country string,phone bigint,creditlimit
    float,checknum string,checkamt int,ordernumber STRING,shippeddate date,status string, comments string,productcode
    string,quantityordered int,priceeach double,orderlinenumber int,productName STRING,productLine STRING,productScale
    STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice double,MSRP double,orderdate
    date) stored as orcfile location 'hdfs://localhost:54310/user/hive/warehouse/retail_mart.db/custordfinal_new/'""")
    hivedata=custdetcomplextypes.join(orddetcomplextypes,'customerNumber')
    hivedata.createTempView("complextypes")
    hivedata.coalesce(1).write.mode("overwrite").format("orc").saveAsTable("retail_mart.custordfinal_new")
	#Load to Elastic search
    custfinales=hivedata.select("customernumber","customername","city",fn.col("quantityInStock").cast(IntegerType()),fn.col("MSRP").cast(DoubleType()),fn.col("orderdate").cast(DateType()))
    writetoes(custfinales)
    print("""Calling the writetoes function to load the custfinales dataframe into Elastic search index""")


if __name__ == "__main__":
    main()