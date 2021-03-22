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
package com.github.datasource.hdfs

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import org.apache.hadoop.fs.BlockLocation
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.types._

/** A scan object that works on HDFS files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsScan(schema: StructType,
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

  private def createPartitions(blockMap: Map[String, Array[BlockLocation]],
                               store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    if (options.containsKey("partitions") &&
          options.get("partitions").toInt == 1) {
      // Generate one partition per file
      for ((fName, blockList) <- blockMap) {
        a += new HdfsPartition(index = 0, offset = 0, length = store.getLength(fName),
                               name = fName)
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        // Generate one partition per hdfs block.
        for (block <- blockList) {
          a += new HdfsPartition(index = i, offset = block.getOffset, length = block.getLength,
                                 name = fName)
          i += 1
        }
      }
    }
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
    var store: HdfsStore = HdfsStoreFactory.getStore(schema, options,
                                                     filters, prunedSchema,
                                                     pushedAggregation)
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    createPartitions(blocks, store)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory =
          new HdfsPartitionReaderFactory(schema, options, filters,
                                       prunedSchema,
                                       pushedAggregation)
}

/** Creates a factory for creating HdfsPartitionReader objects
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsPartitionReaderFactory(schema: StructType,
                                 options: util.Map[String, String],
                                 filters: Array[Filter],
                                 prunedSchema: StructType,
                                 pushedAggregation: Aggregation)
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdfsPartitionReader(schema, options, filters,
                          prunedSchema, partition.asInstanceOf[HdfsPartition],
                          pushedAggregation)
  }
}

/** PartitionReader of HdfsPartitions
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param partition the HdfsPartition to read from
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsPartitionReader(schema: StructType,
                          options: util.Map[String, String],
                          filters: Array[Filter],
                          prunedSchema: StructType,
                          partition: HdfsPartition,
                          pushedAggregation: Aggregation)
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  /* We setup a rowIterator and then read/parse
   * each row as it is asked for.
   */
  private var store: HdfsStore = HdfsStoreFactory.getStore(schema, options,
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
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
                  s" ${partition.length} ${partition.name} index: ${index}")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
