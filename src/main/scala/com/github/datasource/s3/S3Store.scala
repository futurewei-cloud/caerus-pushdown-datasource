// scalastyle:off
/*
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on
package com.github.datasource.s3

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.model.SelectObjectContentEvent
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor
import com.amazonaws.services.s3.model.SelectRecordsInputStream
import com.github.datasource.common.Pushdown
import com.github.datasource.common.Select
import com.github.datasource.common.TypeCast
import com.github.datasource.parse.RowIteratorFactory
import org.apache.commons.csv._
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{Aggregation, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/** A Factory to fetch the correct type of
 *  store object depending on the expected output type
 *  to be sent back from the store.
 */
object S3StoreFactory{
  /** Returns the store object which can process
   *  the input format from params("format").
   *  Currently only csv, json, parquet are supported.
   *
   * @param pushdown object handling filter, project and aggregate pushdown
   * @param params the parameters including those to construct the store
   * @return a new HdfsStore object constructed with above parameters.
   */
  def getS3Store(pushdown: Pushdown,
                 params: java.util.Map[String, String]): S3Store = {

    var format = params.get("format")
    format.toLowerCase(Locale.ROOT) match {
      case "csv" => new S3StoreCSV(pushdown, params)
      case "json" => new S3StoreJSON(pushdown, params)
      case "parquet" => new S3StoreParquet(pushdown, params)
    }
  }
}
/** An abstract hdfs store object which can connect
 *  to an hdfs endpoint, specified by params("endpoint")
 *  Other parameters are parmas("path")
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param params the parameters including those to construct the store
 */
abstract class S3Store(pushdown: Pushdown,
                       params: java.util.Map[String, String]) {

  protected var path = params.get("path")
  protected val logger = LoggerFactory.getLogger(getClass)
  def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }
  protected val s3Credential = new BasicAWSCredentials(params.get("accessKey"),
                                                       params.get("secretKey"))
  protected val s3Client = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                               params.get("endpoint"), Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(staticCredentialsProvider(s3Credential))
    .withClientConfiguration(new ClientConfiguration().withRequestTimeout(24 * 3600 * 1000)
                                                      .withSocketTimeout(24 * 3600* 1000))
    .build()

  def getReader(partition: S3Partition): BufferedReader;
  def getRowIter(partition: S3Partition): Iterator[InternalRow];

  def getNumRows(): Int = {
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(pushdown.schema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    var rows : Int = 0
    do {
      result = s3Client.listObjectsV2(req)
      result.getObjectSummaries().asScala.foreach(objectSummary => {
        val in = s3Client.selectObjectContent(
          Select.requestCount(
            objectSummary.getBucketName(),
            objectSummary.getKey(),
            params,
            pushdown.schema)
        ).getPayload().getRecordsInputStream()
        val countStr: String = IOUtils.toString(in, StandardCharsets.UTF_8);
        rows = countStr.filter(_ >= ' ').toInt
        logger.trace("count is:" + rows)
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    rows
  }

  /** Returns a list of S3ObjectSummary objects for
   *  all the files specified by s3URI's key.
   *
   * @return the array of S3ObjectSummary
   */
  def getObjectSummaries(): Array[S3ObjectSummary] = {
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(pushdown.schema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    var rows : Int = 0
    var objectSummaries : ArrayBuffer[S3ObjectSummary] = new ArrayBuffer[S3ObjectSummary]()
    do {
      result = s3Client.listObjectsV2(req)
      result.getObjectSummaries().asScala.foreach(objectSummary => {
        if (!objectSummary.getKey().contains(".crc") &&
             (objectSummary.getKey().contains(".csv") ||
              objectSummary.getKey().contains(".tbl") )) {
          objectSummaries += objectSummary
        }
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    objectSummaries.toArray
  }
}
/** A S3 protocol store, which can be used to read a partition with
 * any of various pushdowns.
 *
 * @param pushdown object that implements details of pushdown
 * @param params the parameters such as endpoint, etc.
 */
class S3StoreCSV(pushdown: Pushdown,
                 params: java.util.Map[String, String])
                 extends S3Store(pushdown, params) {

  override def toString() : String = "S3StoreCSV" + params + pushdown.filters.mkString(", ")
  def drainInputStream(in: SelectRecordsInputStream): Unit = {
    var buf : Array[Byte] = new Array[Byte](9192);
      var n = 0;
      var totalBytes = 0
      do {
        n = in.read(buf)
        totalBytes += n
      } while (n > -1)
    // logger.info("current: " + n + " Total bytes: " + totalBytes)
  }
  /** Returns a reader for a given S3 partition.
   *  Determines the correct start offset by looking backwards
   *  to find the end of the prior line.
   *  Helps in cases where the last line of the prior partition
   *  was split on the partition boundary.  In that case, the
   *  prior partition's last (incomplete) is included in the next partition.
   *
   * @param partition the partition to read
   * @return a new BufferedReader for this partition.
   */
  def getReader(partition: S3Partition): BufferedReader = {
    var params: Map[String, String] = Map("" -> "")
    new BufferedReader(new InputStreamReader(
      s3Client.selectObjectContent(
            Select.requestCSV(pushdown, partition)
      ).getPayload().getRecordsInputStream(new SelectObjectContentEventVisitor() {
        override def visit(event: SelectObjectContentEvent.RecordsEvent) {
          var data = event.getPayload().array()
          S3StoreCSV.currentTransferLength += data.length
        }
       })))
  }
  /** Returns an Iterator over InternalRow for a given Hdfs partition.
   *
   * @param partition the partition to read
   * @return a new CsvRowIterator for this partition.
   */
  def getRowIter(partition: S3Partition): Iterator[InternalRow] = {
    RowIteratorFactory.getIterator(getReader(partition),
                                   pushdown.readSchema,
                                   params.get("format"))
  }
  logger.trace("S3StoreCSV: schema " + pushdown.schema)
  logger.trace("S3StoreCSV: path " + params.get("path"))
  logger.trace("S3StoreCSV: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreCSV: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreCSV: filters: " + pushdown.filters.mkString(", "))
}

class S3StoreJSON(pushdown: Pushdown,
                  params: java.util.Map[String, String])
  extends S3Store(pushdown, params) {

  override def toString() : String = "S3StoreJSON" + params + pushdown.filters.mkString(", ")

  def getReader(partition: S3Partition): BufferedReader = {
    var params: Map[String, String] = Map("" -> "")
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(pushdown.readSchema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))
    new BufferedReader(new InputStreamReader(
      s3Client.selectObjectContent(
            Select.requestJSON(pushdown, partition)
      ).getPayload().getRecordsInputStream()))
  }
  // TBD for JSON, stubbed out for now.
  def getRowIter(partition: S3Partition): Iterator[InternalRow] = {
    RowIteratorFactory.getIterator(getReader(partition),
                                   pushdown.readSchema,
                                   params.get("format"))
  }
  logger.trace("S3StoreJSON: schema " + pushdown.schema)
  logger.trace("S3StoreJSON: path " + params.get("path"))
  logger.trace("S3StoreJSON: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreJSON: accessKey/secretKey " +
               params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreJSON: filters: " + pushdown.filters.mkString(", "))
}

class S3StoreParquet(pushdown: Pushdown,
                     params: java.util.Map[String, String])
  extends S3Store(pushdown, params) {

  override def toString() : String = "S3StoreParquet" + params + pushdown.filters.mkString(", ")

  def getReader(partition: S3Partition): BufferedReader = {
    var params: Map[String, String] = Map("" -> "")
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(pushdown.readSchema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))
    new BufferedReader(new InputStreamReader(
      s3Client.selectObjectContent(
            Select.requestParquet(pushdown, partition)
      ).getPayload().getRecordsInputStream()))
  }
  def getRowIter(partition: S3Partition): Iterator[InternalRow] = {
    RowIteratorFactory.getIterator(getReader(partition),
                                   pushdown.readSchema,
                                   params.get("format"))
  }
  logger.trace("S3StoreParquet: schema " + pushdown.schema)
  logger.trace("S3StoreParquet: path " + params.get("path"))
  logger.trace("S3StoreParquet: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreParquet: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreParquet: filters: " + pushdown.filters.mkString(", "))
}


object S3StoreCSV {
  private var currentTransferLength: Double = 0
  def resetTransferLength : Unit = { currentTransferLength = 0 }
  def getTransferLength : Double = { currentTransferLength }
}

/** Related routines for the S3 connector.
 *
 */
object S3Store {
  /** Returns true if pushdown is supported by this flavor of
   *  filesystem represented by a string of "filesystem://filename".
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def pushdownSupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").contains("s3a://")) {
      true
    } else {
      false
    }
  }
}
