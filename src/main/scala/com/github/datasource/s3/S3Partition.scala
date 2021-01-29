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

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition
import com.github.datasource.common.PushdownPartition

/** Represents a partition on an S3 store.
 *
 * @param index the position in order of partitions.
 * @param rowOffset offset from start of file in units of number of rows.
 * @param numRows the number of rows in the partition
 * @param onlyPartition true if this is the only partition
 * @param bucket the name of the directory or s3 "bucket"
 * @param key the name of the filename or s3 "key"
 */
class S3Partition(var index: Int,
                  var rowOffset: Long = 0, var numRows: Long = 0,
                  var onlyPartition: Boolean = true,
                  var bucket: String = "",
                  var key: String = "")
  extends Partition with InputPartition with PushdownPartition {

  override def toString() : String = {
    s"""S3Partition index ${index} bucket: ${bucket} key: ${key}"""
  }

  override def preferredLocations(): Array[String] = {
    Array("localhost")
  }
  /** Returns the query clause needed to target this specific partition.
   *
   *  @param partition the S3Partition that is being targeted.
   *
   *  @return String the query clause for use on this partition.
   */
  override def getObjectClause(partition: PushdownPartition): String = {
    val part = partition.asInstanceOf[S3Partition]
    // Treat this as the one and only partition.
    // Some sources like minio can't handle partitions, so this
    // gives us a way to be compatible with them.
    if (part.onlyPartition) {
      "S3Object"
    } else {
      s"""(SELECT * FROM S3Object LIMIT ${part.numRows} OFFSET ${part.rowOffset})"""
    }
  }
}
