# Scopus Application Using Apache Hive, Impala Shell, Spark, Pyspark and Spark - SQL

## Apache - Hive
### Create External Table and Load Data From HDFS
![image](https://user-images.githubusercontent.com/28688869/139133857-8efe0750-c77f-443e-b3fe-53fe678f34aa.png)

### Create Internal Table and Load Data From HDFS
![image](https://user-images.githubusercontent.com/28688869/139133889-14079e23-d446-4633-867c-3bcb7a27d434.png)

### Create Partition Table and Load Data From HDFS
![image](https://user-images.githubusercontent.com/28688869/139133919-0f51c3ab-1db8-4d04-bf2e-7281225f5e20.png)

### Create Cluster Table and Load Data From HDFS
![image](https://user-images.githubusercontent.com/28688869/139133969-4541a898-846f-4c76-80f8-240ef75af295.png)

### Query 1
![image](https://user-images.githubusercontent.com/28688869/139134005-0dcfa0cf-f3bb-40ba-8ed8-ceb5d53c2c64.png)

### Query 2
![image](https://user-images.githubusercontent.com/28688869/139134043-bbaddfda-9565-4c00-92b6-8ea5ad0943c8.png)

### Query 3
![image](https://user-images.githubusercontent.com/28688869/139134067-5bf2f475-3130-4e09-b874-efd6bd26a904.png)



## Impala Shell
![image](https://user-images.githubusercontent.com/28688869/139133123-14c4294a-90e6-4661-b1b8-cd39cad62120.png)

![image](https://user-images.githubusercontent.com/28688869/139133207-242c11e0-a82a-4456-a9c3-13b6a077cdab.png)

![image](https://user-images.githubusercontent.com/28688869/139133248-bbf6fee3-0a26-4b6a-a1a4-2833239838f5.png)

## Spark - Shell
![image](https://user-images.githubusercontent.com/28688869/139133300-89e65fec-046e-45bd-a0ce-8622867b456d.png)

## Pyspark
![image](https://user-images.githubusercontent.com/28688869/139133344-d727835c-701d-43c2-98c4-d462ccd2434a.png)

## Spark - SQL
![image](https://user-images.githubusercontent.com/28688869/139133437-dbca6801-4e85-4ec3-ace0-c2bdb9702597.png)

## Final Result
![image](https://user-images.githubusercontent.com/28688869/139134386-d0696f8e-c879-41bd-ac6d-93e1d46c623d.png)

## Conclusion
From my experiment, Spark SQL is the fastest to query among others. As seen in the SQL for Hive and Impala, eventhough they use SQL the speed can't be compared 
with using Spark SQL. This is probably because of Spark can connect to jdbc as well as SQL or MYSQL. For long running, Spark is faster as it used massively parallel system. Hive and Impala can use 1 core per queries but Spark can use most of the cores on all cluster nodes provided in the system. 

The usage of time execution is taken from Apache Spark Server History. To justify this is because if the usage of NanoTime or other functions to take the execution time. It will be plus minus 2 to 3 seconds which is not accurate for me. So to solve that by using Apache Spark Server History to see the time taken for execution is more accurate in my view.
