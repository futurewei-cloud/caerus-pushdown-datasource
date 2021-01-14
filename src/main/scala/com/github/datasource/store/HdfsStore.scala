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
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{Aggregation, Filter}
import org.apache.spark.sql.types._

/** A Factory to fetch the correct type of
 *  store object depending on the expected output type
 *  to be sent back from the store.
 */
object HdfsStoreFactory {
  /** Returns the store object which can process
   *  the input format from params("format").
   *  Currently only csv is supported.
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

    var format = params.get("format")
    format.toLowerCase(Locale.ROOT) match {
      case "csv" => new HdfsStoreCSV(schema, params, filters, prunedSchema,
                                   pushedAggregation)
    }
  }
}
/** An abstract hdfs store object which can connect
 *  to an hdfs endpoint, specified by params("endpoint")
 *  Other parameters are parmas("path")
 *
 * @param schema the StructType schema to construct store with.
 * @param params the parameters including those to construct the store
 * @param filters the filters to pass down to the store.
 * @param prunedSchema the schema to pushdown (column pruning)
 * @param pushedAggregation the aggregate operations to pushdown
 */
abstract class HdfsStore(schema: StructType,
                         params: java.util.Map[String, String],
                         filters: Array[Filter],
                         prunedSchema: StructType,
                         pushedAggregation: Aggregation) {

  protected var path = params.get("path")
  protected var endpoint = "webhdfs://" + params.get("endpoint") + ":9870"
  protected val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Hdfs Created")
  // SocketTImeout (24 * 3600 * 1000)
  protected val (readColumns: String,
                 readSchema: StructType) = {
    var (columns, updatedSchema) = 
      Pushdown.getColumnSchema(pushedAggregation, prunedSchema)
    (columns,
     if (updatedSchema.names.isEmpty) schema else updatedSchema)
  }
  protected val fileSystem: WebHdfsFileSystem = {
    val conf = new Configuration()
    val hdfsCoreSitePath = new Path("/home/rob/config/core-client.xml")
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0")
    conf.addResource(hdfsCoreSitePath)
    FileSystem.get(URI.create(endpoint), conf)
                   .asInstanceOf[WebHdfsFileSystem]
  }
  def getReader(partition: HdfsPartition): BufferedReader;
  def getRowIter(partition: HdfsPartition): Iterator[InternalRow];
  def getFilePath(fileName: String) : String = {  
    endpoint + "/" + fileName.replace("hdfs://", "")
  }

  /** Returns a list of BlockLocation object representing
   *  all the hdfs blocks in a file.
   *
   * @param fileName the full filename path
   * @return BlockLocation objects for the file
   */
  def getBlockList(fileName: String) : Array[BlockLocation] = {
    val fileToRead = new Path(endpoint + "/" + fileName)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    println(fileStatus)
    val fileBlocks: Array[BlockLocation] = 
      fileSystem.getFileBlockLocations(fileToRead, 0, fileStatus.getLen())

    /* for (block <- fileBlocks) {
      val names = block.getNames
      println("offset: " + block.getOffset + " length: " + block.getLength
              + " location: " + names(0))
    }*/
    fileBlocks
  }

  /** Returns the length of the file in bytes.
   *
   * @param fileName the full path of the file
   * @return byte length of the file.
   */
  def getLength(fileName: String) : Long = {
    val fileToRead = new Path(endpoint + "/" + fileName)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    fileStatus.getLen
  }
}
/** A hdfs store, which can be used to read a partition with
 * any of various pushdowns.
 *
 * @param schema to apply to this store
 * @param params the parameters such as endpoint, etc.
 * @param filters the filters to pushdown to endpoint
 * @param prunedSchema
 * @param pushedAggregation
 */
class HdfsStoreCSV(var schema: StructType,
                   params: java.util.Map[String, String],
                   filters: Array[Filter],
                   var prunedSchema: StructType,
                   pushedAggregation: Aggregation)
  extends HdfsStore(schema, params, filters, prunedSchema,
                    pushedAggregation) {

  override def toString() : String = "HdfsStoreCSV" + params + filters.mkString(", ")

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
    val filePath = new Path(endpoint + "/" + partition.name)
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
      val inStrm = fileSystem.open(filePath)
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
  def getReader(partition: HdfsPartition): BufferedReader = {
    val filePath = new Path(endpoint + "/" + partition.name)
    val inStrm = fileSystem.open(filePath)
    val startOffset = getStartOffset(partition)
    inStrm.seek(startOffset)
    val partitionLength = (partition.offset + partition.length) - startOffset
    new BufferedReader(new InputStreamReader(new BoundedInputStream(inStrm, partitionLength)))
  }
  /** Returns an Iterator over InternalRow for a given Hdfs partition.
   *
   * @param partition the partition to read
   * @return a new CsvRowIterator for this partition.
   */
  def getRowIter(partition: HdfsPartition): Iterator[InternalRow] = {
    new CSVRowIterator(getReader(partition), readSchema)
  }
}