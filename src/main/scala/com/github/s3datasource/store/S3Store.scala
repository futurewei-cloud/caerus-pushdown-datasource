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
package com.github.s3datasource.store

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

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.ClientConfiguration
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

object S3StoreFactory{
  def getS3Store(schema: StructType,
                 params: java.util.Map[String, String],
                 filters: Array[Filter],
                 prunedSchema: StructType,
                 pushedAggregation: Aggregation): S3Store = {

    var format = params.get("format")
    format.toLowerCase(Locale.ROOT) match {
      case "csv" => new S3StoreCSV(schema, params, filters, prunedSchema,
                                   pushedAggregation)
      case "json" => new S3StoreJSON(schema, params, filters, prunedSchema,
                                     pushedAggregation)
      case "parquet" => new S3StoreParquet(schema, params, filters, prunedSchema,
                                           pushedAggregation)
    }
  }
}
abstract class S3Store(schema: StructType,
                       params: java.util.Map[String, String],
                       filters: Array[Filter],
                       prunedSchema: StructType,
                       pushedAggregation: Aggregation) {

  protected var path = params.get("path")
  protected val logger = LoggerFactory.getLogger(getClass)
  def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }
  logger.trace("S3Store Created")

  protected val s3Credential = new BasicAWSCredentials(params.get("accessKey"),
                                                     params.get("secretKey"))
  protected val s3Client = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                               params.get("endpoint"), Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(staticCredentialsProvider(s3Credential))
    .withClientConfiguration(new ClientConfiguration().withRequestTimeout(5 * 60 * 1000)
                                                      .withSocketTimeout(5 * 60 * 1000))
    .build()
  protected val (readColumns: String,
                 readSchema: StructType) = {
    var (columns, updatedSchema) = 
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    (columns,
     if (updatedSchema.names.isEmpty) schema else updatedSchema)
  }

  def getRows(partition: S3Partition): ArrayBuffer[InternalRow];
  def getReader(partition: S3Partition): BufferedReader;
  def getRowIter(partition: S3Partition): Iterator[InternalRow];

  def getNumRows(): Int = {
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(schema.fields.map(x => x.name): _*)
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
            schema)
        ).getPayload().getRecordsInputStream()
        val countStr: String = IOUtils.toString(in, StandardCharsets.UTF_8);
        rows = countStr.filter(_ >= ' ').toInt
        logger.trace("count is:" + rows)
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    rows
  }

  def getObjectSummaries(): Array[S3ObjectSummary] = {
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(schema.fields.map(x => x.name): _*)
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
        objectSummaries += objectSummary
        // logger.info("file is:" + objectSummary.getKey())
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    objectSummaries.toArray
  }
}

class S3StoreCSV(var schema: StructType,
                 params: java.util.Map[String, String],
                 filters: Array[Filter],
                 var prunedSchema: StructType,
                 pushedAggregation: Aggregation)
                 extends S3Store(schema, params, filters, prunedSchema, 
                                 pushedAggregation) {

  override def toString() : String = "S3StoreCSV" + params + filters.mkString(", ")
  def drainInputStream(in: SelectRecordsInputStream) = {
    var buf : Array[Byte] = new Array[Byte](9192);            
      var n = 0;
      var totalBytes = 0
      do {
        n = in.read(buf)
        totalBytes += n
      } while (n > -1)
    //logger.info("current: " + n + " Total bytes: " + totalBytes)
  }
  def getReader(partition: S3Partition): BufferedReader = {
    var params: Map[String, String] = Map("" -> "")
    new BufferedReader(new InputStreamReader(
      s3Client.selectObjectContent(
            Select.requestCSV(partition.bucket,
                              partition.key,
                              params,
                              schema,
                              readSchema,
                              readColumns,
                              filters,
                              pushedAggregation,
                              partition)
      ).getPayload().getRecordsInputStream(new SelectObjectContentEventVisitor() {
        override def visit(event: SelectObjectContentEvent.RecordsEvent) {
          var data = event.getPayload().array()
          S3StoreCSV.currentTransferLength += data.length
        }
       })))
  }
  def getRowIter(partition: S3Partition): Iterator[InternalRow] = {
    new CSVRowIterator(getReader(partition), readSchema)
  }
  override def getRows(partition: S3Partition): ArrayBuffer[InternalRow] = {
    val numRows = getNumRows()
    var records = new ArrayBuffer[InternalRow](numRows)
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var params: Map[String, String] = Map("" -> "")

    req.withBucketName(partition.bucket)
    req.withPrefix(partition.key)
    req.withMaxKeys(1000)

    val (columns, updatedSchema) =
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    val readSchema = if (updatedSchema.names.isEmpty) schema else updatedSchema
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(readSchema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))
    val in = s3Client.selectObjectContent(
      Select.requestCSV(partition.bucket,
                        partition.key,
                        params,
                        schema,
                        readSchema,
                        columns,
                        filters,
                        pushedAggregation,
                        partition)
      ).getPayload().getRecordsInputStream()
    var parser = CSVParser.parse(in, java.nio.charset.Charset.forName("UTF-8"), csvFormat)
    var index: Int = 0
    try {
      for (record <- parser.asScala) {
        records += InternalRow.fromSeq(readSchema.fields.map(x => {
          TypeCast.castTo(record.get(x.name), x.dataType, x.nullable)
        }))
        if ((index % 500000) == 0) {
          logger.info("partition: " + partition.index + " index: " + index)
        }
        index += 1
      }
    } catch {
      case NonFatal(e) => logger.error(s"Exception while parsing ", e)
    }
    logger.info("getRows() partition: " + partition.index + " total rows:" + index)
    parser.close()
    //logger.info(records.mkString(", "))
    records
  }
  logger.trace("S3StoreCSV: schema " + schema)
  logger.trace("S3StoreCSV: path " + params.get("path"))
  logger.trace("S3StoreCSV: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreCSV: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreCSV: filters: " + filters.mkString(", "))
}

class S3StoreJSON(schema: StructType,
                  params: java.util.Map[String, String],
                  filters: Array[Filter],
                  prunedSchema: StructType,
                  pushedAggregation: Aggregation)
  extends S3Store(schema, params, filters, prunedSchema, pushedAggregation) {

  override def toString() : String = "S3StoreJSON" + params + filters.mkString(", ")
  
  def getReader(partition: S3Partition): BufferedReader = {
    var params: Map[String, String] = Map("" -> "")
    val (columns, updatedSchema) =
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    val readSchema = if (updatedSchema.names.isEmpty) schema else updatedSchema
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(readSchema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))
    new BufferedReader(new InputStreamReader(
      s3Client.selectObjectContent(
            Select.requestJSON(partition.bucket,
                              partition.key,
                              params,
                              schema,
                              readSchema,
                              columns,
                              filters,
                              pushedAggregation,
                              partition)
                              
      ).getPayload().getRecordsInputStream()))
  }
  // TBD for JSON, stubbed out for now.
  def getRowIter(partition: S3Partition): Iterator[InternalRow] = {
    new CSVRowIterator(getReader(partition), readSchema)
  }
  override def getRows(partition: S3Partition): ArrayBuffer[InternalRow] = {
    var records = new ArrayBuffer[InternalRow]
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    val (columns, updatedSchema) =
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    do {
      result = s3Client.listObjectsV2(req)
      asScalaBuffer(result.getObjectSummaries()).foreach(objectSummary => {
        val br = new BufferedReader(new InputStreamReader(
          s3Client.selectObjectContent(
            Select.requestJSON(
              objectSummary.getBucketName(),
              objectSummary.getKey(),
              params,
              schema,
              prunedSchema,
              columns,
              filters,
              pushedAggregation,
              partition)
          ).getPayload().getRecordsInputStream()))
        var line : String = null
        while ( {line = br.readLine(); line != null}) {
          var row = new Array[Any](schema.fields.length)
          var rowValues = line.split(",")
          var index = 0
          while (index < rowValues.length) {
            val field = schema.fields(index)
            row(index) = TypeCast.castTo(rowValues(index), field.dataType,
              field.nullable)
            index += 1
          }
          records += InternalRow.fromSeq(row)
        }
        br.close()
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    records
  }
  logger.trace("S3StoreJSON: schema " + schema)
  logger.trace("S3StoreJSON: path " + params.get("path"))
  logger.trace("S3StoreJSON: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreJSON: accessKey/secretKey " +
               params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreJSON: filters: " + filters.mkString(", "))
}

class S3StoreParquet(schema: StructType,
                     params: java.util.Map[String, String],
                     filters: Array[Filter],
                     prunedSchema: StructType,
                     pushedAggregation: Aggregation)
  extends S3Store(schema, params, filters, prunedSchema, pushedAggregation) {

  override def toString() : String = "S3StoreParquet" + params + filters.mkString(", ")
  
  def getReader(partition: S3Partition): BufferedReader = {
    var params: Map[String, String] = Map("" -> "")
    val (columns, updatedSchema) =
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    val readSchema = if (updatedSchema.names.isEmpty) schema else updatedSchema
    val csvFormat = CSVFormat.DEFAULT
      .withHeader(readSchema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))
    new BufferedReader(new InputStreamReader(
      s3Client.selectObjectContent(
            Select.requestParquet(partition.bucket,
                              partition.key,
                              params,
                              schema,
                              readSchema,
                              columns,
                              filters,
                              pushedAggregation,
                              partition)                              
      ).getPayload().getRecordsInputStream()))
  }
  // TBD for parquet, stubbed out for now.
  def getRowIter(partition: S3Partition): Iterator[InternalRow] = {
    new CSVRowIterator(getReader(partition), readSchema)
  }
  override def getRows(partition: S3Partition): ArrayBuffer[InternalRow] = {
    var records = new ArrayBuffer[InternalRow]
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    val (columns, updatedSchema) =
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)

    do {
      result = s3Client.listObjectsV2(req)
      asScalaBuffer(result.getObjectSummaries()).foreach(objectSummary => {
        val br = new BufferedReader(new InputStreamReader(
          s3Client.selectObjectContent(
            Select.requestParquet(
              objectSummary.getBucketName(),
              objectSummary.getKey(),
              params,
              schema,
              prunedSchema,
              columns,
              filters,
              pushedAggregation,
              partition)
          ).getPayload().getRecordsInputStream()))
        var line : String = null
        while ( {line = br.readLine(); line != null}) {
          var row = new Array[Any](schema.fields.length)
          var rowValues = line.split(",")
          var index = 0
          while (index < rowValues.length) {
            val field = schema.fields(index)
            row(index) = TypeCast.castTo(rowValues(index), field.dataType,
              field.nullable)
            index += 1
          }
          records += InternalRow.fromSeq(row)
        }
        br.close()
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    records
  }
  logger.trace("S3StoreParquet: schema " + schema)
  logger.trace("S3StoreParquet: path " + params.get("path"))
  logger.trace("S3StoreParquet: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreParquet: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreParquet: filters: " + filters.mkString(", "))
}


object S3StoreCSV {
  private var currentTransferLength: Double = 0

  def resetTransferLength : Unit = { currentTransferLength = 0 }
  def getTransferLength : Double = { currentTransferLength }

}
