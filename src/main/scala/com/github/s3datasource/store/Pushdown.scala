// scalastyle:off
/*
 * Copyright 2018 MinIO, Inc.
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
 * 
 * Note that portions of this code came from spark-select code:
 *  https://github.com/minio/spark-select/blob/master/src/main/scala/io/minio/spark/select/FilterPushdown.scala
 * 
 * Other portions of this code, most notably compileAggregates, and getColumnSchema,
 * came from this patch by Huaxin Gao:
 *   https://github.com/apache/spark/pull/29695
 */
// scalastyle:on
package com.github.s3datasource.store

import java.sql.{Date, Timestamp}

import org.slf4j.LoggerFactory

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper methods for pushing filters into Select queries.
 */
object Pushdown {

  protected val logger = LoggerFactory.getLogger(getClass)
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
   * Attempt to convert the given filter into a Select expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val sqlEscapedValue: String = dataType match {
          case StringType => s"""'${value.toString.replace("'", "\\'\\'")}'"""
          case DateType => s""""${value.asInstanceOf[Date]}""""
          case TimestampType => s""""${value.asInstanceOf[Timestamp]}""""
          case _ => value.toString
        }
        s"s." + s""""$attr"""" + s" $comparisonOp $sqlEscapedValue"
      }
    }
    def buildOr(leftFilter: Option[String], rightFilter: Option[String]): Option[String] = {
      val left = leftFilter.getOrElse("")
      val right = rightFilter.getOrElse("")
      Option(s"""( $left OR $right )""")
    }
    def buildAnd(leftFilter: Option[String], rightFilter: Option[String]): Option[String] = {
      val left = leftFilter.getOrElse("")
      val right = rightFilter.getOrElse("")
      Option(s"""( $left AND $right )""")
    }
    def buildNot(filter: Option[String]): Option[String] = {
      val f = filter.getOrElse("")
      Option(s"""NOT ( $f )""")
    }
    filter match {
      case Or(left, right) => buildOr(buildFilterExpression(schema, left),
                                      buildFilterExpression(schema, right))
      case And(left, right) => buildAnd(buildFilterExpression(schema, left),
                                        buildFilterExpression(schema, right))
      case Not(filter) => buildNot(buildFilterExpression(schema, filter))
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case _ => None
    }
  }

  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
  private def buildObjectClause(partition: S3Partition): String = {
    // Treat this as the one and only partition.
    // Some sources like minio can't handle partitions, so this
    // gives us a way to be compatible with them.
    if (partition.onlyPartition) {
      "S3Object"
    } else {
      s"""(SELECT * FROM S3Object LIMIT ${partition.numRows} OFFSET ${partition.rowOffset})"""
    }
  }
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }
  def compileAggregates(aggregates: Seq[AggregateFunc]): 
                       (Map[String, Array[String]], Array[Filter]) = {
    var filters = Array.empty[Filter]
    def quote(colName: String): String = quoteIdentifier(colName)
    val compiledAggregates = aggregates.map {
      case Min(column, isDistinct, filter) =>
        if (filter.nonEmpty) filters +:= filter.get
        if (isDistinct) {
          Some(quote(column) -> s"MIN(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"MIN(${quote(column)})")
        }
      case Max(column, isDistinct, filter) =>
        if (filter.nonEmpty) filters +:= filter.get
        if (isDistinct) {
          Some(quote(column) -> s"MAX(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"MAX(${quote(column)})")
        }
      case Sum(column, isDistinct, _) =>
        if (isDistinct) {
          Some(quote(column) -> s"SUM(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"SUM(${quote(column)})")
        }
      case Avg(column, isDistinct, _) =>
        if (isDistinct) {
          Some(quote(column) -> s"AVG(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"AVG(${quote(column)})")
        }
      case _ => None
    }
    var map: Map[String, Array[String]] = Map()
    if (!compiledAggregates.contains(None)) {
      for (i <- 0 until compiledAggregates.length) {
        val key = compiledAggregates(i).get._1
        val value = map.get(key)
        if (value == None) {
          map += (key -> Array(compiledAggregates(i).get._2))
        } else {
          map += (key -> (value.get :+ compiledAggregates(i).get._2))
        }
      }
    }
    (map, filters)
  }
  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  def getColumnSchema(aggregation: Aggregation,
                      schema: StructType): 
                     (String, StructType, Array[Filter]) = {

    val columnNames = schema.map(_.name).toArray
    val quotedColumns: Array[String] = columnNames.map(colName => quoteIdentifier(colName))
    val compiledAgg = compileAggregates(aggregation.aggregateExpressions)
    val compiledAggregates = compiledAgg._1
    var updatedSchema: StructType = new StructType()
    var updatedFilters = Array.empty[Filter]
    updatedFilters = compiledAgg._2 // filters ++ 
    val flippedMap = compiledAggregates.map(_.swap)
    val colDataTypeMap: Map[String, StructField] = quotedColumns.zip(schema.fields).toMap
    val sb = new StringBuilder()
    var index = 0
    quotedColumns.map(c => compiledAggregates.getOrElse(c, c)).foreach(
      x => x match {
        case str: String =>
          sb.append(", ").append(str)
          val dataField = colDataTypeMap.get(str).get
              updatedSchema = updatedSchema.add(dataField.name + "_" + index,
                                                dataField.dataType,
                                                dataField.nullable)
          index += 1
        case array: Array[String] =>
          sb.append(", ").append(array.mkString(", "))
          for (a <- array) {
            if (a.contains("MAX") || a.contains("MIN")) {
              // get the original column data type
              val dataField = colDataTypeMap.get(flippedMap.get(array).get).get
              updatedSchema = updatedSchema.add(dataField.name + "_" +index,
                                                dataField.dataType,
                                                dataField.nullable)
            } else if (a.contains("SUM")) {
              // Same as Spark, promote to the largest types to prevent overflows.
              // IntegralType: if not Long, promote to Long
              // FractionalType: if not Double, promote to Double
              // DecimalType.Fixed(precision, scale):
              //   follow what is done in Sum.resultType, +10 to precision
              val dataField = colDataTypeMap.get(flippedMap.get(array).get).get
              dataField.dataType match {
              // We cannot access this private class. Disable for now.
              /* case DecimalType.Fixed(precision, scale) =>
                  updatedSchema = updatedSchema.add(
                    dataField.name, DecimalType(precision + 10, scale), dataField.nullable) */
                case _: IntegerType =>
                  updatedSchema = updatedSchema.add(dataField.name + "_" + index,
                                                    LongType, dataField.nullable)
                case _ =>
                  updatedSchema = updatedSchema.add(dataField.name + "_" + index,
                                                    DoubleType, dataField.nullable)
              }
            } else { // AVG
              // Same as Spark, promote to the largest types to prevent overflows.
              // DecimalType.Fixed(precision, scale):
              //   follow what is done in Average.resultType, +4 to precision and scale
              // promote to Double for other data types
              val dataField = colDataTypeMap.get(flippedMap.get(array).get).get
              dataField.dataType match {
                // We cannot access this private class. Disable for now.
                /* case DecimalType.Fixed(p, s) => updatedSchema =
                  updatedSchema.add( 
                    dataField.name, DecimalType(p + 4, s + 4), dataField.nullable) */
                case _ => updatedSchema =
                  updatedSchema.add(dataField.name + "_" + index,
                                    DoubleType, dataField.nullable)
               }
            }
            index += 1
          }
      }
    )
    (if (sb.length == 0) "" else sb.substring(1), 
     if (sb.length == 0) schema else updatedSchema, updatedFilters)
  }

  def quoteIdentifierGroupBy(colName: String): String = {
    s""""$colName""""
  }
  private def getGroupByClause(aggregation: Aggregation): String = {
    if (aggregation.groupByExpressions.length > 0) {
      val quotedColumns = aggregation.groupByExpressions.map(quoteIdentifierGroupBy)
      s"GROUP BY ${quotedColumns.mkString(", ")}"
    } else {
      ""
    }
  }
  def queryFromSchema(schema: StructType,
                      prunedSchema: StructType,
                      columns: String,
                      filters: Array[Filter],
                      aggregation: Aggregation,
                      partition: S3Partition): String = {
    var columnList = prunedSchema.fields.map(x => s"s." + s""""${x.name}"""").mkString(",")

    if (columns.length > 0) {
      columnList = columns
    } else if (columnList.length == 0) {
      columnList = "*"
    }
    val whereClause = buildWhereClause(schema, filters)
    val objectClause = buildObjectClause(partition)
    var retVal = ""
    val groupByClause = getGroupByClause(aggregation)
    if (whereClause.length == 0) {
      retVal = s"SELECT $columnList FROM $objectClause s $groupByClause"
    } else {
      retVal = s"SELECT $columnList FROM $objectClause s $groupByClause $whereClause"
    }
    logger.info(s"""SQL Query partition(${partition.index}:${partition.key}): 
                 |${retVal}""".stripMargin);
    retVal
  }

}
