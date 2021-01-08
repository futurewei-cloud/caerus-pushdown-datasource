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
package com.github.datasource.store

import java.sql.{Date, Timestamp}
import java.util.{Locale, StringTokenizer}

import scala.collection.mutable.ArrayBuilder

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
      case IsNull(attr) => Option(s"${attr} IS NULL")
      case IsNotNull(attr) => Option(s"${attr} IS NOT NULL")
      case StringStartsWith(attr, value) => Option(s"${attr} LIKE '${value}%'")
      case StringEndsWith(attr, value) => Option(s"${attr} LIKE '%${value}'")
      case StringContains(attr, value) => Option(s"${attr} LIKE '%${value}%'")
      case other@_ => { logger.info("unknown filter:" + other) ; None }
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
  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  def getColumnSchema(aggregation: Aggregation,
                      schema: StructType): 
                     (String, StructType) = {

    val compiledAgg = compileAggregates(aggregation.aggregateExpressions)
    val sb = new StringBuilder()
    val columnNames = schema.map(_.name).toArray
    var updatedSchema: StructType = new StructType()
    if (compiledAgg.length == 0) {
      updatedSchema = schema
      columnNames.foreach(x => sb.append(",").append(x))
    } else {
      updatedSchema = getAggregateColumnsList(sb, aggregation, schema, compiledAgg)
    }
    (if (sb.length == 0) "" else sb.substring(1), 
     if (sb.length == 0) schema else updatedSchema)
  }
  def compileAggregates(aggregates: Seq[AggregateFunc]): (Array[String]) = {
    def quote(colName: String): String = quoteIdentifier(colName)
    val aggBuilder = ArrayBuilder.make[String]
    aggregates.map {
      case Min(column) =>
        if (!column.contains("+") && !column.contains("-") && !column.contains("*")
          && !column.contains("/")) {
          aggBuilder += s"MIN(${quote(column)})"
        } else {
          aggBuilder += s"MIN(${quoteEachCols(column)})"
        }
      case Max(column) =>
        if (!column.contains("+") && !column.contains("-") && !column.contains("*")
          && !column.contains("/")) {
          aggBuilder += s"MAX(${quote(column)})"
        } else {
          aggBuilder += s"MAX(${quoteEachCols(column)})"
        }
      case Sum(column) =>
        if (!column.contains("+") && !column.contains("-") && !column.contains("*")
          && !column.contains("/")) {
          aggBuilder += s"SUM(${quote(column)})"
        } else {
          aggBuilder += s"SUM(${quoteEachCols(column)})"
        }
      case Avg(column) =>
        if (!column.contains("+") && !column.contains("-") && !column.contains("*")
          && !column.contains("/")) {
          aggBuilder += s"AVG(${quote(column)})"
        } else {
          aggBuilder += s"AVG(${quoteEachCols(column)})"
        }
      case _ =>
    }
    aggBuilder.result
  }

  private def quoteEachCols (column: String): String = {
    def quote(colName: String): String = quoteIdentifier(colName)
    val colsBuilder = ArrayBuilder.make[String]
    val st = new StringTokenizer(column, "+-*/", true)
    colsBuilder += quote(st.nextToken().trim)
    while (st.hasMoreTokens) {
      colsBuilder += st.nextToken
      colsBuilder +=  quote(st.nextToken().trim)
    }
    colsBuilder.result.mkString(" ")
  }

  private def getAggregateColumnsList(sb: StringBuilder,
                                      aggregation: Aggregation, schema: StructType, 
                                      compiledAgg: Array[String]) = {
    val columnNames = schema.map(_.name).toArray
    val quotedColumns: Array[String] = columnNames.map(colName => quoteIdentifier(colName.toLowerCase(Locale.ROOT)))
    val colDataTypeMap: Map[String, StructField] = quotedColumns.zip(schema.fields).toMap
    val newColsBuilder = ArrayBuilder.make[String]
    var updatedSchema: StructType = new StructType()
    for (col <- compiledAgg) {
      newColsBuilder += col
    }
    for (groupBy <- aggregation.groupByExpressions) {
      newColsBuilder += quoteIdentifier(groupBy)
    }

    val newColumns = newColsBuilder.result
    sb.append(", ").append(newColumns.mkString(", "))

    // build new schemas
    for (c <- newColumns) {
      val colName: Array[String] = if (!c.contains("+") && !c.contains("-") && !c.contains("*")
        && !c.contains("/")) {
        if (c.contains("MAX") || c.contains("MIN") || c.contains("SUM") || c.contains("AVG")) {
          Array(c.substring(c.indexOf("(") + 1, c.indexOf(")")))
        } else {
          Array(c)
        }
      } else {
        val colsBuilder = ArrayBuilder.make[String]
        val st = new StringTokenizer(c.substring(c.indexOf("(") + 1, c.indexOf(")")), "+-*/", false)
        while (st.hasMoreTokens) {
          colsBuilder += st.nextToken.trim
        }
        colsBuilder.result
      }

      if (c.contains("MAX") || c.contains("MIN")) {
        updatedSchema = updatedSchema
          .add(getDataType(colName, colDataTypeMap))
      } else if (c.contains("SUM")) {
        // Same as Spark, promote to the largest types to prevent overflows.
        // IntegralType: if not Long, promote to Long
        // FractionalType: if not Double, promote to Double
        // DecimalType.Fixed(precision, scale):
        //   follow what is done in Sum.resultType, +10 to precision
        val dataField = getDataType(colName, colDataTypeMap)
        dataField.dataType match {
          // We cannot access these private classes. Disable for now.
          /* case DecimalType.Fixed(precision, scale) =>
            updatedSchema = updatedSchema.add(
              dataField.name, DecimalType.bounded(precision + 10, scale), dataField.nullable)
          case _: IntegralType => */
          case _: IntegerType =>
            updatedSchema = updatedSchema.add(dataField.name, LongType, dataField.nullable)
          case _ =>
            updatedSchema = updatedSchema.add(dataField.name, DoubleType, dataField.nullable)
        }
      } else if (c.contains("AVG")) { // AVG
        // Same as Spark, promote to the largest types to prevent overflows.
        // DecimalType.Fixed(precision, scale):
        //   follow what is done in Average.resultType, +4 to precision and scale
        // promote to Double for other data types
        val dataField = getDataType(colName, colDataTypeMap)
        dataField.dataType match {
          // We cannot access ese  private classes. Disable for now.
          /* case DecimalType.Fixed(p, s) => updatedSchema =
            updatedSchema.add(
              dataField.name, DecimalType.bounded(p + 4, s + 4), dataField.nullable) */
          case _ => updatedSchema =
            updatedSchema.add(dataField.name, DoubleType, dataField.nullable)
        }
      } else {
        updatedSchema = updatedSchema.add(colDataTypeMap.get(c).get)
      }
    }
    updatedSchema
  }

  private def contains(s1: String, s2: String, checkParathesis: Boolean): Boolean = {
    if (false /*SQLConf.get.caseSensitiveAnalysis*/) {
      if (checkParathesis) s1.contains("(" + s2) else s1.contains(s2)
    } else {
      if (checkParathesis) {
        s1.toLowerCase(Locale.ROOT).contains("(" + s2.toLowerCase(Locale.ROOT))
      } else {
        s1.toLowerCase(Locale.ROOT).contains(s2.toLowerCase(Locale.ROOT))
      }
    }
  }

  private def getDataType(
      cols: Array[String],
      colDataTypeMap: Map[String, StructField]): StructField = {
    if (cols.length == 1) {
      colDataTypeMap.get(cols(0).toLowerCase(Locale.ROOT)).get
    } else {
      val map = new java.util.HashMap[Object, Integer]
      map.put(ByteType, 0)
      map.put(ShortType, 1)
      map.put(IntegerType, 2)
      map.put(LongType, 3)
      map.put(FloatType, 4)
      map.put(DecimalType, 5)
      map.put(DoubleType, 6)
      var colType = colDataTypeMap.get(cols(0).toLowerCase(Locale.ROOT)).get
      for (i <- 1 until cols.length) {
        val dType = colDataTypeMap.get(cols(i).toLowerCase(Locale.ROOT)).get
        if (dType.dataType.isInstanceOf[DecimalType]
          && colType.dataType.isInstanceOf[DecimalType]) {
          if (dType.dataType.asInstanceOf[DecimalType].precision
            > colType.dataType.asInstanceOf[DecimalType].precision) {
            colType = dType
          }
        } else {
          if (map.get(colType.dataType) < map.get(dType.dataType)) {
            colType = dType
          }
        }
      }
      colType
    }
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
      retVal = s"SELECT $columnList FROM $objectClause s $whereClause $groupByClause"
    }
    logger.info(s"""SQL Query partition(${partition.index}:${partition.key}): 
                 |${retVal}""".stripMargin);
    retVal
  }

}
