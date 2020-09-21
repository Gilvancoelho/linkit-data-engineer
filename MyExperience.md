# Linkit-data-engineer 

I Began at Saturday, due a fully last week (was my last sprint week, sorry) and started reading all requirements and understand implementations that need to be done. 
I draw a small schema with everything that I was planning to do. 

1 I choose to download and spin up a Cloudera QuickStart Cluster in Docker Container in my local machine. 
The environment had some issues due a limited memory in my personal computer but due a limited time I decided to keep working in a Docker.

**About the Architecture defined:**

a) I choose to have 3 files one for each App and 2 aditional files, a Connection trait object and a package Object. SparkConn.scala has a trait object that can be
extended by other objects and classes in order to avoid a lot of diferent connections and manage resources. 

I choose a trait once this object can be used to be inherited to classes that already extends other classes.
Package object have been choose in order to keep all connection data and commom variables in my package. I did that thinking in let easy if 
a need to change my server having a single point to change my variables.

b) Hive: Some adjustments need to be done in a columns name that use "-" and data have been loaded to Hive.

c) When I did a aggregation this could be done in Spark or Hive, but i choose to submit a SQL to Hive, once Hive can handle Huge amount of data, of course, 
much slow than Spark but with a balance between resource and time.

d) To send data to hbase I Choose to create a composite rowKey with 4 Columns : "driverId", "truckID", "eventID","eventTime". This approach has the following
considerations: 
1) rowkey is the unique index at Hbase table and search by index is efficient. And I put the most likely search columns in a rowkey. 
2) Index design is so important to balanace data distribution and this index design will ensure that we have a distributed information in big data env.

e) Regarding to publish information in a Kafka i choose to use an Apache/NiFi container just due my lack of time. To act as subscriber I built a Spark Streaming 
simple enought to read and write information. Much more complexity could be added, like windowning, agregations and so on, but there are no requirements
to do that.

f) I tried to Log operations and document methods with the time that I had. Today, Monday, I just made final adjustments and send project to github in 
order to share the main files.  

Thanks for your time!

**I am available to clarify some doubts. Regards! Gilvan**
