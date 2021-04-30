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
package com.github.datasource.common

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

  protected var supportsIsNull = true

  def setSupportsIsNull(supports: Boolean): Unit = {
    supportsIsNull = supports
  }
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
  /** returns the representation of the column name according to the
   *  current option set.
   *  @param schema - Schema of table
   *  @param attr - Attribute name
   *  @return String - representation of the column name.
   */
  def getColString(schema: StructType, attr: String): String = {
    s"_${getSchemaIndex(schema, attr)}"
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
        s"s." + s""""${getColString(schema, attr)}"""" + s" $comparisonOp $sqlEscapedValue"
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
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNull(attr) => if (supportsIsNull) {
          Option(s"${getColString(schema, attr)} IS NULL")
        } else {
          Option("TRUE")
        }
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNotNull(attr) => if (supportsIsNull) {
          Option(s"${getColString(schema, attr)} IS NOT NULL")
        } else {
          Option("TRUE")
        }
      case StringStartsWith(attr, value) =>
        Option(s"${getColString(schema, attr)} LIKE '${value}%'")
      case StringEndsWith(attr, value) =>
        Option(s"${getColString(schema, attr)} LIKE '%${value}'")
      case StringContains(attr, value) =>
        Option(s"${getColString(schema, attr)} LIKE '%${value}%'")
      case other@_ => logger.info("unknown filter:" + other) ; None
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
  def quoteIdentifier(colName: String,
                      schema: StructType): String = {
    s"${getColString(schema, colName)}"
  }
  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  def getColumnSchema(aggregation: Aggregation,
                      prunedSchema: StructType, schema: StructType):
                     (String, StructType) = {

    val (compiledAgg, aggDataType) = compileAggregates(aggregation.aggregateExpressions,
                                                       schema)
    val sb = new StringBuilder()
    val columnNames = prunedSchema.map(_.name).toArray
    var updatedSchema: StructType = new StructType()
    if (compiledAgg.length == 0) {
      val cols = prunedSchema.fields.map(x => {
            getColString(schema, x.name)
        }).toArray
      updatedSchema = prunedSchema
      cols.foreach(x => sb.append(",").append(x))
    } else {
      updatedSchema = getAggregateColumnsList(sb,
                                              aggregation,
                                              prunedSchema, compiledAgg, aggDataType,
                                              schema)
    }
    (if (sb.length == 0) "" else sb.substring(1),
     if (sb.length == 0) prunedSchema else updatedSchema)
  }
  private def containsArithmeticOp(col: String): Boolean =
    col.contains("+") || col.contains("-") || col.contains("*") || col.contains("/")

  /** Returns an array of aggregates translated to strings.
   *
   * @param aggregates the array of aggregates to translate
   * @return array of strings
   */
  def compileAggregates(aggregates: Seq[AggregateFunc],
                        schema: StructType): (Array[String], Array[DataType]) = {
    def quote(colName: String): String = quoteIdentifier(colName, schema)
    val aggBuilder = ArrayBuilder.make[String]
    val dataTypeBuilder = ArrayBuilder.make[DataType]
    aggregates.map {
      case Min(column, dataType) =>
        dataTypeBuilder += dataType
        if (!containsArithmeticOp(column)) {
          aggBuilder += s"MIN(${quote(column)})"
        } else {
          aggBuilder += s"MIN(${quoteEachCols(column, schema)})"
        }
      case Max(column, dataType) =>
        dataTypeBuilder += dataType
        if (!containsArithmeticOp(column)) {
          aggBuilder += s"MAX(${quote(column)})"
        } else {
          aggBuilder += s"MAX(${quoteEachCols(column, schema)})"
        }
      case Sum(column, dataType, isDistinct) =>
        val distinct = if (isDistinct) "DISTINCT " else ""
        dataTypeBuilder += dataType
        if (!containsArithmeticOp(column)) {
          aggBuilder += s"SUM(${distinct} ${quote(column)})"
        } else {
          aggBuilder += s"SUM(${distinct}${quoteEachCols(column, schema)})"
        }
      case Avg(column, dataType, isDistinct) =>
        val distinct = if (isDistinct) "DISTINCT " else ""
        dataTypeBuilder += dataType
        if (!containsArithmeticOp(column)) {
          aggBuilder += s"AVG(${distinct} ${quote(column)})"
        } else {
          aggBuilder += s"AVG(${distinct}${quoteEachCols(column, schema)})"
        }
      case Count(column, dataType, isDistinct) =>
        val distinct = if (isDistinct) "DISTINCT " else ""
        dataTypeBuilder += dataType
        if (!containsArithmeticOp(column)) {
          aggBuilder += s"COUNT(${distinct}${quote(column)})"
        } else {
          aggBuilder += s"COUNT(${distinct}${quoteEachCols(column, schema)})"
        }
      case _ =>
    }
    (aggBuilder.result, dataTypeBuilder.result)
  }

  private def quoteEachCols (column: String, schema: StructType): String = {
    def quote(colName: String): String = quoteIdentifier(colName, schema)
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
                                      aggregation: Aggregation, prunedSchema: StructType,
                                      compiledAgg: Array[String],
                                      aggDataType: Array[DataType],
                                      schema: StructType) = {
    val columnNames = prunedSchema.map(_.name).toArray
    val quotedColumns: Array[String] =
      columnNames.map(colName => quoteIdentifier(colName.toLowerCase(Locale.ROOT), schema))
    val colDataTypeMap: Map[String, StructField] = quotedColumns.zip(prunedSchema.fields).toMap
    val newColsBuilder = ArrayBuilder.make[String]
    var updatedSchema: StructType = new StructType()

    for ((col, dataType) <- compiledAgg.zip(aggDataType)) {
      newColsBuilder += col
      updatedSchema = updatedSchema.add(col, dataType)
    }
    for (groupBy <- aggregation.groupByExpressions) {
      val quotedGroupBy = quoteIdentifier(groupBy, schema)
      newColsBuilder += quotedGroupBy
      updatedSchema = updatedSchema.add(colDataTypeMap.get(quotedGroupBy).get)
    }
    sb.append(", ").append(newColsBuilder.result.mkString(", "))
    updatedSchema
  }

  private def contains(s1: String, s2: String, checkParathesis: Boolean): Boolean = {
    if (false /* SQLConf.get.caseSensitiveAnalysis */) {
      if (checkParathesis) s1.contains("(" + s2) else s1.contains(s2)
    } else {
      if (checkParathesis) {
        s1.toLowerCase(Locale.ROOT).contains("(" + s2.toLowerCase(Locale.ROOT))
      } else {
        s1.toLowerCase(Locale.ROOT).contains(s2.toLowerCase(Locale.ROOT))
      }
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

  /** returns the index of a field matching an input name in a schema.
   *
   * @param schema the schema to scan
   * @param name the name of the field to search for in schema
   *
   * @return Integer - The index of this field in the input schema.
   */
  def getSchemaIndex(schema: StructType, name: String): Integer = {
    for (i <- 0 to schema.fields.size) {
      if (schema.fields(i).name == name) {
        return i + 1
      }
    }
    -1
  }

  /** Returns a string to represent the input query.
   *
   * @return String representing the query to send to the endpoint.
   */
  def queryFromSchema(schema: StructType,
                      prunedSchema: StructType,
                      columns: String,
                      filters: Array[Filter],
                      aggregation: Aggregation,
                      partition: PushdownPartition): String = {
    var columnList = prunedSchema.fields.map(x => s"s." + s""""${x.name}"""").mkString(",")

    if (columns.length > 0) {
      columnList = columns
    } else if (columnList.length == 0) {
      columnList = "*"
    }
    val whereClause = buildWhereClause(schema, filters)
    val objectClause = partition.getObjectClause(partition)
    var retVal = ""
    val groupByClause = getGroupByClause(aggregation)
    if (whereClause.length == 0) {
      retVal = s"SELECT $columnList FROM $objectClause s $groupByClause"
    } else {
      retVal = s"SELECT $columnList FROM $objectClause s $whereClause $groupByClause"
    }
    logger.info(s"SQL Query partition: ${partition.toString}")
    logger.info(s"SQL Query: ${retVal}")
    retVal
  }
  /** Returns a string to represent the schema of the table.
   *
   * @param schema the StructType representing the definition of columns.
   * @return String representing the table's columns.
   */
  def schemaString(schema: StructType): String = {

    schema.fields.map(x => {
      val dataTypeString = {
        x.dataType match {
        case IntegerType => "INTEGER"
        case LongType => "LONG"
        case DoubleType => "NUMERIC"
        case _ => "STRING"
        }
      }
      s"${x.name} ${dataTypeString}"
    }).mkString(", ")
  }
}
