# ETL CHALLENGE

### Technology

1. Storage - Hadoop HDFS
2. Apache Kafka
3. Apache Spark 3.0.1 (spark Streaming)
4. Python 3
5. VSCode

### prerequisite
To run the code successfully, you need to have hdfs setup on either linux server (possibly Ubuntu on Virtual Box). You need to have Kafka installed as well as pyspark setup. Also, ensure your machine has enough memory for the processes involved. If you're using Ubuntu on Virtual Box, try and increase the base memory to about 4gb, though depends on the size of your pc memory.

Ensure all the Deamons of hadoop are printed out when you typed jps on terminal. You should get something like the following - kafka deamons included too.

stephen@Ubuntu-18:~$ jps  
_3249 DataNode_  
_3458 SecondaryNameNode_  
_3045 NameNode_  
_4390 Kafka_  
_3944 NodeManager_  
_3976 QuorumPeerMain_  
_26588 Jps_  
_3612 ResourceManager_

You can interact with hadoop UI through localhost:50070. The UI should look like below.


• Data Cleanup : Constraint are applied to avoid such as null handling and column uniqueness, before writing data to a designated stora
• Anonymisation : Pernsonal identification details are made hidden based on daily request 
• Logs : Each data stream batch is logged onto the console as shown in the product batch below

