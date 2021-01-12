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
package com.github.datasource

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.github.datasource.store.{HdfsPartition, HdfsStore, HdfsStoreFactory}
import org.slf4j.LoggerFactory

import org.apache.hadoop.fs.BlockLocation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

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

  private def createPartitions(blockList: Array[BlockLocation],
                               fileName: String,
                               store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    if (options.containsKey("partitions") &&
          options.get("partitions").toInt == 1) {   
      a += new HdfsPartition(index = 0, offset = 0, length = store.getLength(fileName),
                             name = fileName)
    } else {
      // Generate one partition per hdfs block.
      for (block <- blockList) {
        a += new HdfsPartition(index = i, offset = block.getOffset, length = block.getLength,
                               name = fileName)
        i += 1
      }
    }
    logger.info(a.mkString(" "))
    a.toArray
  }
  private def getPartitions(): Array[InputPartition] = {
    var store: HdfsStore = HdfsStoreFactory.getStore(schema, options,
                                                     filters, prunedSchema,
                                                     pushedAggregation)
    val fileName = options.get("path").replace("hdfs://", "")                                                 
    val blocks : Array[BlockLocation] = store.getBlockList(fileName)
    createPartitions(blocks, fileName, store)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory =
          new HdfsPartitionReaderFactory(schema, options, filters,
                                       prunedSchema,
                                       pushedAggregation)
}

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
      logger.info(s"""get: partition: ${partition.index} ${partition.offset} ${partition.length}""" + 
                  s""" ${partition.name} index: ${index}""")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
