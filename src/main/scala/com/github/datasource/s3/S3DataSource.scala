/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasource.s3

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/** A scan object that works on S3 files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class S3Scan(schema: StructType,
             options: util.Map[String, String],
             filters: Array[Filter], prunedSchema: StructType,
             pushedAggregation: Aggregation)
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def readSchema(): StructType = prunedSchema

  override def toBatch: Batch = this

  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = getPartitions()

  private def generateFilePartitions(objectSummary : S3ObjectSummary): Array[InputPartition] = {
    var store: S3Store = S3StoreFactory.getS3Store(schema, options,
                                                   filters, prunedSchema,
                                                   pushedAggregation)
    var numPartitions: Int = 
      if (options.containsKey("partitions") &&
          options.get("partitions").toInt != 0) {
        options.get("partitions").toInt
      } else {
        (objectSummary.getSize() / maxPartSize +
        (if ((objectSummary.getSize() % maxPartSize) == 0) 0 else 1)).toInt
      }
    if (numPartitions == 0) {
      throw new ArithmeticException("numPartitions is 0")
    }
    var totalRows = if (numPartitions == 1) 0 else store.getNumRows()
    val partitionRows = totalRows / numPartitions
    var partitionArray = new ArrayBuffer[InputPartition](0)
    logger.debug(s"""Num Partitions ${numPartitions}""")
    for (i <- 0 to (numPartitions - 1)) {
      val rows = {
        if (i == numPartitions - 1) {
          totalRows - (i * partitionRows)
        }
        else partitionRows
      }
      val nextPart = new S3Partition(index = i,
                                     rowOffset = i * partitionRows,
                                     numRows = rows,
                                     onlyPartition = (numPartitions == 1),
                                     bucket = objectSummary.getBucketName(), 
                                     key = objectSummary.getKey()).asInstanceOf[InputPartition]
      partitionArray += nextPart
      logger.info(nextPart.toString)
    }
    partitionArray.toArray
  }
  private def createS3Partitions(objectSummaries : Array[S3ObjectSummary]): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    // In this case we generate one partition per file.
    for (summary <- objectSummaries) {
      a += new S3Partition(index = i, bucket = summary.getBucketName(), key = summary.getKey())
      i += 1
    }
    logger.info(a.mkString(" "))
    a.toArray
  }

  /** Returns an Array of S3Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of S3Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    var store: S3Store = S3StoreFactory.getS3Store(schema, options, filters,
                                                   prunedSchema,
                                                   pushedAggregation)
    val objectSummaries : Array[S3ObjectSummary] = store.getObjectSummaries()

    // If there is only one file, we will partition it as needed automatically.
    if (objectSummaries.length == 1) {
      generateFilePartitions(objectSummaries(0))
    } else {
      // If there are multiple files we treat each one as a partition.
      createS3Partitions(objectSummaries)
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory =
          new S3PartitionReaderFactory(schema, options, filters,
                                       prunedSchema,
                                       pushedAggregation)
}

/** Creates a factory for creating S3PartitionReader objects
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class S3PartitionReaderFactory(schema: StructType,
                               options: util.Map[String, String],
                               filters: Array[Filter],
                               prunedSchema: StructType,
                               pushedAggregation: Aggregation)
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new S3PartitionReader(schema, options, filters,
                          prunedSchema, partition.asInstanceOf[S3Partition],
                          pushedAggregation)
  }
}

/** PartitionReader of S3Partitions
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param partition the S3Partition to read from
 * @param pushedAggregation the array of aggregations to push down
 */
class S3PartitionReader(schema: StructType,
                        options: util.Map[String, String],
                        filters: Array[Filter],
                        prunedSchema: StructType,
                        partition: S3Partition,
                        pushedAggregation: Aggregation)
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  logger.trace("Created")

  /* We setup a rowIterator and then read/parse
   * each row as it is asked for.
   */
  private var store: S3Store = S3StoreFactory.getS3Store(schema, options, 
                                                         filters, prunedSchema,
                                                         pushedAggregation)
  private var rowIterator: Iterator[InternalRow] = store.getRowIter(partition)

  var index = 0
  def next: Boolean = {
    rowIterator.hasNext
  }
  def get: InternalRow = {
    val row = rowIterator.next
    if (((index % 500000) == 0) ||
        (!next)) {
      logger.info(s"""get: partition: ${partition.index} ${partition.bucket} """ + 
                  s"""${partition.key} index: ${index}""")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
