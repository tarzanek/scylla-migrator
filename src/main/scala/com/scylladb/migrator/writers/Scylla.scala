package com.scylladb.migrator.writers

import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ ColumnDef, Schema }
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CopyType, Default, Rename, TargetSettings }
import com.scylladb.migrator.readers.Cassandra.log
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.LogManager
import org.apache.spark.sql.cassandra.DataTypeConverter
import org.apache.spark.sql.types.{ IntegerType, LongType, StructField, StructType }
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

object Scylla {
  val log = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")

  def writeDataframe(
    target: TargetSettings.Scylla,
    renames: List[Rename],
    defaults: List[Default],
    df: DataFrame,
    timestampColumns: Option[TimestampColumns],
    tokenRangeAccumulator: Option[TokenRangeAccumulator])(implicit spark: SparkSession): Unit = {
    val connector = Connectors.targetConnector(spark.sparkContext.getConf, target)
    val writeConf = WriteConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(
        ttl = timestampColumns.map(_.ttl).fold(TTLOption.defaultValue)(TTLOption.perRow),
        timestamp = timestampColumns
          .map(_.writeTime)
          .fold(TimestampOption.defaultValue)(TimestampOption.perRow)
      )

    val tableDef =
      connector.withSessionDo(Schema.tableFromCassandra(_, target.keyspace, target.table))
    log.info("TableDef retrieved for target:")
    log.info(tableDef)

    // Similarly to createDataFrame, when using withColumnRenamed, Spark tries
    // to re-encode the dataset. Instead we just use the modified schema from this
    // DataFrame; the access to the rows is positional anyway and the field names
    // are only used to construct the columns part of the INSERT statement.
    var renamedSchema = renames
      .foldLeft(df) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }
      .schema

    val targetSchema = StructType(tableDef.columns.map(DataTypeConverter.toStructField))

    for (default <- defaults) {
      val index = targetSchema.fieldIndex(default.column)
      val column = targetSchema.fields(index)
      renamedSchema = renamedSchema.add(column)
    }

    log.info("Schema after renames:")
    log.info(renamedSchema.treeString)

    val columnSelector = SomeColumns(renamedSchema.fields.map(_.name: ColumnRef): _*)

    // Spark's conversion from its internal Decimal type to java.math.BigDecimal
    // pads the resulting value with trailing zeros corresponding to the scale of the
    // Decimal type. Some users don't like this so we conditionally strip those.
    val rdd =
      if (!target.stripTrailingZerosForDecimals) df.rdd
      else
        df.rdd.map { row =>
          Row.fromSeq(row.toSeq.map {
            case x: java.math.BigDecimal => x.stripTrailingZeros()
            case x                       => x
          })
        }
    val fieldIndices = renamedSchema.fields.map(_.name).toList.zipWithIndex.toMap
    // Introduce a defaults list, which will set a column with a default value
    val frdd =
      if (defaults.size < 1) rdd
      else {
        rdd.map { row =>
          var frow = row.copy()
// TODO optimize below, traversing seq once might be faster than calling updated few times
          for (default <- defaults) {
            val index = fieldIndices.getOrElse(default.column, -1)
            if (index == -1) { // missing in target DB schema, noop, log warning? perhaps quit earlier, already in driver
              frow = Row.fromSeq(frow.toSeq)
            } else if (index >= row.size) { //new column added
              frow = Row.fromSeq(frow.toSeq ++ Seq(default.default))
            } else if (row.isNullAt(index)) { //set default for null columns from original schema
              frow = Row.fromSeq(frow.toSeq.updated(index, default.default))
            }
          }
          frow
        }
      }

    frdd
      .saveToCassandra(
        target.keyspace,
        target.table,
        columnSelector,
        writeConf,
        tokenRangeAccumulator = tokenRangeAccumulator
      )(connector, SqlRowWriter.Factory)
  }

}
