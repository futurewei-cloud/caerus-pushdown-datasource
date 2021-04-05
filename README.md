This is a new Spark V2 datasource, which has support for
pushdown of filter, project and aggregate.
This data source can operate against either HDFS or S3.

In the case of S3, this data source is compatible with the
S3 server included here:
https://github.com/futurewei-cloud/caerus-dikeCS

In the case of HDFS, this data source is compatible with the
HDFS server with proxy configuration located here:
https://github.com/futurewei-cloud/caerus-dikeHDFS

How to use
=============
Build using: sbt package

The datasource requires the following below jars and packages
- pushdown-datasource_2.12-0.1.0.jar 
- ndp-hdfs-1.0.jar
- AWS Java SDK
- Hadoop version, compatible with Spark.

For working examples of this datasource, use the below caerus-dike repo to lauch Spark.  
One example of this is here:
https://github.com/futurewei-cloud/caerus-dike/blob/master/spark/examples/scala/run_example.sh

Or just follow the README.md to run a demo.
https://github.com/futurewei-cloud/caerus-dike/blob/master/README.md

Use of datasource
=================
Working examples that use this datasource can be found here:
https://github.com/futurewei-cloud/caerus-dike/blob/master/spark/examples/scala/

The data source can be used with the following parameters to the spark session's read command.

```
spark.read
     .format("com.github.datasource")
     .schema(schema)
     .option("format", "tbl")
     .load("ndphdfs://hdfs-server/table.tbl")
```

Supported Protocols
====================
The datasource supports either S3 or HDFS.

For HDFS, we would provide a Spark Session load of something like this:

```
.load("ndphdfs://hdfs-server/table.tbl")
```

Note that the ndphdfs is provided by ndp-hdfs-1.0.jar, the ndp-hdfs client.

For S3, we would provide a Spark Session load of something like this:

```
.load("s3a://s3-server/table.tbl")
```
Other options
=============

We support options to disable pushdown of either filter, project and aggregate.
These can be disabled individually or in any combination.
By default all three pushdowns are enabled.

To disable all pushdowns:

```
spark.read
     .format("com.github.datasource")
     .schema(schema)
     .option("format", "tbl")
     .option("DisableFilterPush", "")
     .option("DisableProjectPush", "")
     .option("DisableAggregatePush", "")
     .load("ndphdfs://hdfs-server/table.tbl")
```

To disable just aggregate:

```
spark.read
     .format("com.github.datasource")
     .schema(schema)
     .option("format", "tbl")
     .option("DisableAggregatePush", "")
     .load("ndphdfs://hdfs-server/table.tbl")
```

Supported formats
=================
Currently, only .tbl, a pipe (|) deliminated format is supported.  This is the format used by the TPCH benchmark.

Credits
========
We based some of the code on minio's spark select.
https://github.com/minio/spark-select

