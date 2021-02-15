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

package com.github.datasource.parse

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import java.util.Locale
import java.util

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import com.github.datasource.common.TypeCast

class Delimiters(var fieldDelim: Char,
                 var lineDelim: Char = '\n',
                 var quoteDelim: Char = '\"')

/** A Factory to fetch the correct type of
 *  row Iterator object depending on the file format.
 *
 * RowIteratorFactory.getIterator(rowReader, schema, params)
 */
object RowIteratorFactory {
  /** Returns the Iterator object which can process
   *  the input format from params("format").
   *  Currently only csv and tbl is supported.
   *
   * @param schema the StructType schema to construct store with.
   * @param params the parameters including those to construct the store
   * @param rowReader the BufferedReader that has the
   * @return a new Iterator of InternalRow constructed with above parameters.
   */
  def getIterator(rowReader: BufferedReader,
                  schema: StructType,
                  format: String): Iterator[InternalRow] = {
    format.toLowerCase(Locale.ROOT) match {
      case "csv" => new RowIterator(rowReader, schema, new Delimiters(','))
      case "tbl" => new RowIterator(rowReader, schema, new Delimiters('|'))
    }
  }
}

/** Iterator object that allows for parsing
 *  tbl rows into InternalRow structures.
 *
 * @param rowReader the bufferedReader to fetch data
 * @param schema the format of this stream of data
 */
class RowIterator(rowReader: BufferedReader,
                  schema: StructType,
                  delim: Delimiters)
  extends Iterator[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  /** Returns an InternalRow parsed from the input line.
   *
   * @param line the String of line to parse
   * @return the InternalRow of this line..
   */
  private def parseLine(line: String): InternalRow = {
    var row = new Array[Any](schema.fields.length)
    var value: String = ""
    var index = 0
    var fieldStart = 0
    // println("parseLine: " + line)
    while (index < schema.fields.length && fieldStart < line.length) {
      if (line(fieldStart) != delim.quoteDelim) {
        var fieldEnd = line.substring(fieldStart).indexOf(delim.fieldDelim)
        if (fieldEnd == -1) {
          // field is from here to the end of the line
          value = line.substring(fieldStart)
          // Next field start is after comma
          fieldStart = line.length
        } else {
          // field is from start (no skipping) to just before ,
          value = line.substring(fieldStart, fieldStart + fieldEnd)
          // Next field start is after comma
          fieldStart = fieldStart + fieldEnd + 1
        }
      } else {
        // Search from +1 (after ") to next quote
        var fieldEnd = line.substring(fieldStart + 1).indexOf(delim.quoteDelim)
        // Field range is from after " to just before (-1) next quote
        value = line.substring(fieldStart + 1, fieldStart + fieldEnd + 1)
        // Next field start is after quote and comma
        fieldStart = fieldStart + 1 + fieldEnd + 2
      }
      val field = schema.fields(index)
      try {
        row(index) = TypeCast.castTo(value, field.dataType,
                                     field.nullable)
      } catch {
        case e: Throwable => println(s"Exception found parsing index: ${index} field: ${field.name} value: ${value}")
                  println(s"line: ${line}")
                  throw e
      }
      index += 1
    }
    /* We will simply discard the row since
     * the next partition will pick up this row.
     * This can be expected for some protocols, thus there is no tracing by default.
     */
    if (index < schema.fields.length) {
      //println(s"line too short ${index}/${schema.fields.length}: ${line}")
      InternalRow.empty
    } else {
      InternalRow.fromSeq(row.toSeq)
    }
  }

  /** Returns the next row or if none, InternalRow.empty.
   *
   * @return InternalRow for the next row.
   */
  private var nextRow: InternalRow = {
    val firstRow = getNextRow()
    firstRow
  }
  /** Returns row following the current one, 
   *  (if availble), by parsing the next line.
   *
   * @return the next InternalRow object or InternalRow.empty if none.
   */
  private var rows: Long = 0
  private var lastRow: InternalRow = InternalRow.empty
  private def getNextRow(): InternalRow = {
    var line: String = null
    if ({line = rowReader.readLine(); (line == null)}) {
      // println("last Part " + name + " total rows: " + RowIterator.getRows() + " rows: " + rows + 
      //        " last row: " + lastRow.toString)
      // if (RowIterator.getDebugWriter.isDefined) RowIterator.getDebugWriter.get.close
      InternalRow.empty
    } else {
      val row = parseLine(line)
      if (RowIterator.getDebugWriter.isDefined && row.numFields > 0) {
        RowIterator.getDebugWriter.get.write(row.toString() + "\n")
        RowIterator.getDebugWriter.get.flush
        RowIterator.incRows()
        rows += 1
      } else if (row.numFields == 0) {
        // println("Part" + name + " total rows: " + RowIterator.getRows() + " rows: " + rows + 
        //        " last row: " + lastRow.toString)
        // if (RowIterator.getDebugWriter.isDefined) RowIterator.getDebugWriter.get.close
      }
      lastRow = row
      row
    }
  }
  /** Returns true if there are remaining rows.
   *
   * @return true if rows remaining, false otherwise.
   */
  override def hasNext: Boolean = {
    nextRow.numFields > 0
  }
  /** Returns the following InternalRow
   *
   * @return the next InternalRow or InternalRow.empty if none.
   */
  override def next: InternalRow = {
    val row = nextRow
    nextRow = getNextRow()
    row
  }
  def close(): Unit = {}
}

object RowIterator {
  private var debugFile: String = ""
  private var totalRows: Long = 0
  private var debugWriter: Option[FileWriter] = None
  
  def setDebugFile(file:String): Unit = {
    debugFile = file
    val outFile = new File(RowIterator.debugFile + ".txt")
    debugWriter = Some(new FileWriter(outFile))
  }
  def getDebugWriter(): Option[FileWriter] = debugWriter
  def incRows() : Unit = { totalRows += 1}
  def getRows() : Long = totalRows
}
