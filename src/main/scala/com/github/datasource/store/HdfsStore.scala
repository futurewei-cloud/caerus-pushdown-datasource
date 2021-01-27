// scalastyle:off
/*
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
package com.github.datasource.store

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.hdfs.web.DikeHdfsFileSystem
import org.apache.hadoop.hdfs.web.TokenAspect
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{Aggregation, Filter}
import org.apache.spark.sql.types._
import com.github.datasource.parse._

/** A Factory to fetch the correct type of
 *  store object.
 */
object HdfsStoreFactory {
  /** Returns the store object.
   *
   * @param schema the StructType schema to construct store with.
   * @param params the parameters including those to construct the store
   * @param filters the filters to pass down to the store.
   * @param prunedSchema the schema to pushdown (column pruning)
   * @param pushedAggregation the aggregate operations to pushdown
   * @return a new HdfsStore object constructed with above parameters.
   */
  def getStore(schema: StructType,
               params: java.util.Map[String, String],
               filters: Array[Filter],
               prunedSchema: StructType,
               pushedAggregation: Aggregation): HdfsStore = {
    new HdfsStore(schema, params, filters, prunedSchema,
                  pushedAggregation)
  }
}
/** A hdfs store object which can connect
 *  to a file on hdfs filesystem, specified by params("path"),
 *  And which can read a partition with any of various pushdowns.
 *
 * @param schema the StructType schema to construct store with.
 * @param params the parameters including those to construct the store
 * @param filters the filters to pass down to the store.
 * @param prunedSchema the schema to pushdown (column pruning)
 * @param pushedAggregation the aggregate operations to pushdown
 */
class HdfsStore(schema: StructType,
                params: java.util.Map[String, String],
                filters: Array[Filter],
                prunedSchema: StructType,
                pushedAggregation: Aggregation) {

  override def toString() : String = "HdfsStore" + params + filters.mkString(", ")
  protected val path = params.get("path")
  protected val endpoint = {
    val server = path.split("/")(2)
    if (path.contains("dikehdfs://")) {
      ("dikehdfs://" + server + ":9860")
    } else if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + ":9870")
    } else {
      ("hdfs://" + server + ":9000")
    }
  }
  val filePath = {
    val server = path.split("/")(2)
    if (path.contains("dikehdfs://")) {
      val str = path.replace("dikehdfs://" + server, "dikehdfs://" + server + ":9860")
      str
    } else if (path.contains("webhdfs")) {
      path.replace(server, server + ":9870")
    } else {
      path.replace(server, server + ":9000")
    }
  }
  protected val logger = LoggerFactory.getLogger(getClass)
  protected val (readColumns: String,
                 readSchema: StructType) = {
    var (columns, updatedSchema) = 
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    (columns,
     if (updatedSchema.names.isEmpty) schema else updatedSchema)
  }
  protected val fileSystem = {
    val conf = new Configuration()
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0")
    conf.set("fs.dikehdfs.impl", classOf[org.apache.hadoop.hdfs.web.DikeHdfsFileSystem].getName)

    if (path.contains("http://dikehdfs")) {
      val fs = FileSystem.get(URI.create(endpoint), conf)
      fs.asInstanceOf[DikeHdfsFileSystem]
    } else {
      FileSystem.get(URI.create(endpoint), conf)
    }
  }
  protected val fileSystemType = fileSystem.getScheme

  /** Returns a reader for a given Hdfs partition.
   *  Determines the correct start offset by looking backwards
   *  to find the end of the prior line.
   *  Helps in cases where the last line of the prior partition
   *  was split on the partition boundary.  In that case, the
   *  prior partition's last (incomplete) is included in the next partition.
   *
   * @param partition the partition to read
   * @return a new BufferedReader for this partition.
   */
  def getReader(partition: HdfsPartition, startOffset: Long = 0): BufferedReader = {
    val filePath = new Path(partition.name)
    val readParam = {
      if (fileSystemType != "dikehdfs" || params.containsKey("DisableProcessor")) {
        ""
      } else {
        val (requestQuery, requestSchema) =  {
          if (fileSystemType == "dikehdfs") {
            (Pushdown.queryFromSchema(schema, readSchema, readColumns,
                                      filters, pushedAggregation, partition),
            Pushdown.schemaString(schema))
          } else {
            ("", "")
          }
        }
        new ProcessorRequest(requestSchema, requestQuery).toXml
      }
    }
    val inStrm = {
      if (fileSystemType == "dikehdfs" && !params.containsKey("DisableProcessor")) {
        val fs = fileSystem.asInstanceOf[DikeHdfsFileSystem]
        fs.open(filePath, 4096, readParam).asInstanceOf[FSDataInputStream]
      } else {
        fileSystem.open(filePath)
      }
    }
    inStrm.seek(startOffset)
    val partitionLength = (partition.offset + partition.length) - startOffset
    new BufferedReader(new InputStreamReader(new BoundedInputStream(inStrm, partitionLength)))
  }
  /** Returns a list of BlockLocation object representing
   *  all the hdfs blocks in a file.
   *
   * @param fileName the full filename path
   * @return BlockLocation objects for the file
   */
  def getBlockList(fileName: String) : Array[BlockLocation] = {
    val fileToRead = new Path(fileName)
    val fileStatus = fileSystem.getFileStatus(fileToRead)

    // Use MaxValue to indicate we want info on all blocks.
    fileSystem.getFileBlockLocations(fileToRead, 0, Long.MaxValue)
  }
  /** Returns the length of the file in bytes.
   *
   * @param fileName the full path of the file
   * @return byte length of the file.
   */
  def getLength(fileName: String) : Long = {
    val fileToRead = new Path(filePath)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    fileStatus.getLen
  }
  /** Returns the offset in bytes to start reading
   *   an hdfs partition.
   *  This takes into account any prior lines that might be incomplete
   *  from the prior partition.
   *
   * @param partition the partition to find start for
   * @return offset to begin reading partition
   */
  @throws(classOf[Exception])
  def getStartOffset(partition: HdfsPartition) : Long = {
    if (fileSystemType == "dikehdfs" && !params.containsKey("DisableProcessor")) {
      // No need to find offset, dike server does this under the covers for us.
      // When DikeProcessor is disabled, we need to deal with partial lines for ourselves.
      return 0
    }
    val currentPath = new Path(filePath)
    if (partition.offset == 0) 0 else {
      /* When we are not the first partition,
       * read the end of the last partition and find the prior line break.
       * The prior partition will stop at the end of the partition and discard
       * the partial line.  This partition will include that prior line.
       */
      val partitionBytes = 1024
      val priorBytes = 1024
      var lineEnd = -1
      val bufferBytes = {
        if (partition.length > partitionBytes) {
          priorBytes + partitionBytes
        } else
          priorBytes + partition.length.asInstanceOf[Int]
      }
      val buffer = new Array[Byte](bufferBytes)
      val inStrm = fileSystem.open(currentPath)
      inStrm.seek(partition.offset)
      inStrm.readFully(partition.offset - priorBytes, buffer)
      for (i <- priorBytes to 0 by -1) {
        if (buffer(i) == '\n') {
          return partition.offset - (priorBytes.asInstanceOf[Long] - (i.asInstanceOf[Long] + 1))
        }
      }
      if (lineEnd == -1) throw new Exception("line end not found")
      lineEnd
    }
  }
  /** Returns an Iterator over InternalRow for a given Hdfs partition.
   *
   * @param partition the partition to read
   * @return a new CsvRowIterator for this partition.
   */
  def getRowIter(partition: HdfsPartition): Iterator[InternalRow] = {
    RowIteratorFactory.getIterator(getReader(partition, getStartOffset(partition)),
                                   readSchema,
                                   params.get("format"))
  }
}

/** Related routines for the HDFS connector.
 *
 */
object HdfsStore {

  /** Returns true if pushdown is supported by this flavor of
   *  filesystem represented by a string of "filesystem://filename".
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def pushdownSupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").contains("dikehdfs://")) {
      true
    } else {
      // other filesystems like hdfs and webhdfs do not support pushdown.
      false
    }
  }  
}
