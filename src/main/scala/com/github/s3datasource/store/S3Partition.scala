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
package com.github.s3datasource.store

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

class S3Partition(var index: Int,
                  var rowOffset: Long, var numRows: Long,
                  var onlyPartition: Boolean)
  extends Partition with InputPartition {

  override def toString() : String = {
    s"""S3Partition index ${index}
        rowOffset: ${rowOffset}
        numRows: ${numRows}
        onlyPartition: ${onlyPartition}"""
  }

  override def preferredLocations(): Array[String] = {
    Array("localhost")
  }
}
