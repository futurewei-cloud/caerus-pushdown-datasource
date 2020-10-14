This is a spark V2 data source for the S3 API.

We based some of the code on minio's spark select.
https://github.com/minio/spark-select

Build using sbt package.

To use with spark include the following with spark-submit:
--jars s3datasource_2.12-0.1.0.jar

There is a working example here:
https://github.com/peterpuhov-github/dike/tree/master/spark/examples/scala
