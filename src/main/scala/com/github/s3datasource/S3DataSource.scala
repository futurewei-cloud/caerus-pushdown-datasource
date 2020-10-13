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
package com.github.s3datasource

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import com.github.s3datasource.store.{S3Partition, S3Store, S3StoreFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class DefaultSource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("\n\n\n\n\n\n\nS3 Data Source Created")
  override def toString: String = s"S3DataSource()"
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }

  override def getTable(schema: StructType,
                        transforms: Array[Transform],
                        options: util.Map[String, String]): Table = {
    logger.trace("getTable: Options " + options)
    // logger.info("getTable " + schema)
    new S3BatchTable(schema, options)
  }

  override def keyPrefix(): String = {
    "s3"
  }
  override def shortName(): String = "s3datasource"
}

class S3BatchTable(schema: StructType,
                   options: util.Map[String, String])
  extends Table with SupportsRead {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(params: CaseInsensitiveStringMap): ScanBuilder =
      new S3ScanBuilder(schema, options)
}

class S3ScanBuilder(schema: StructType,
                    options: util.Map[String, String])
  extends ScanBuilder with SupportsPushDownFilters {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")

  var scanFilters: Array[Filter] = new Array[Filter](0)

  override def build(): Scan = new S3SimpleScan(schema, options, scanFilters)

  def pushedFilters: Array[Filter] = {
    logger.trace("pushedFilters" + scanFilters.toList)
    scanFilters
  }

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.trace("pushFilters" + filters.toList)
    if (options.containsKey("DisablePushDown")) {
      filters
    } else {
      scanFilters = filters
      // return empty array to indicate we pushed down all the filters.
      // new Array[Filter](0)

      // For now return all filters to indicate they need to be re-evaluated.
      scanFilters
    }
  }
}

class S3SimpleScan(schema: StructType,
                   options: util.Map[String, String],
                   filters: Array[Filter])
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  private var partitions: Array[InputPartition] = getPartitions()

  private def getPartitions(): Array[InputPartition] = {
    var store: S3Store = S3StoreFactory.getS3Store(schema, options, filters)
    var totalRows = store.getNumRows()
    var numPartitions = options.getOrDefault("partitions", "4").toInt
    val partitionRows = totalRows / numPartitions
    var a = new Array[InputPartition](0)
    logger.debug(s"""Num Partitions ${numPartitions}""")
    for (i <- 0 to numPartitions - 1) {
      val rows = {
        if (i == numPartitions - 1) {
          totalRows - (i * partitionRows)
        }
        else partitionRows
      }
      logger.debug(s"""Partition ${i} rowOffset ${i * partitionRows} numRows ${rows}""")
      a :+= (new S3Partition(i,
                             i * partitionRows,
                             rows,
                             numPartitions == 1)).asInstanceOf[InputPartition]
      logger.info(a.mkString(" "))
    }
    a
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory =
          new S3PartitionReaderFactory(schema, options, filters)
}

class S3PartitionReaderFactory(schema: StructType,
                               options: util.Map[String, String],
                               filters: Array[Filter])
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new S3PartitionReader(schema, options, filters, partition.asInstanceOf[S3Partition])
  }
}

class S3PartitionReader(schema: StructType,
                        options: util.Map[String, String],
                        filters: Array[Filter],
                        partition: S3Partition)
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  logger.trace("Created")

  /* We pull in the entire data set as a list.
   * Then we return the data one row as a time as requested
   * Through the iterator interface.
   */
  private var store: S3Store = S3StoreFactory.getS3Store(schema, options, filters)
  private var initted: Boolean = false
  private var rows: ArrayBuffer[InternalRow] = ArrayBuffer.empty[InternalRow]
  private var length: Int = 0
  // logger.trace("rows " + rows.mkString(", "))

  var index = 0
  def next: Boolean = {
    if (!initted) {
      // read in the rows as they are needed.
      rows = store.getRows(partition)
      length = rows.length
      initted = true
    }
    index < length
  }

  def get: InternalRow = {
    val row = rows(index)
    if (((index % 500000) == 0) ||
        (index == (length - 1))) {
      logger.info(s"""partition: ${partition.index} get index: ${index}""")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
