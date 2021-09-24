package com.scylladb.migrator.writers

import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.CassandraOption
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CopyType, Rename, TargetSettings }
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

import java.sql.Timestamp

object Scylla {
  val log = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")

  val emonths = Timestamp.valueOf("2021-01-01 00:00:00")
  val tmonths = Timestamp.valueOf("2020-09-01 00:00:00")
  val dummyTS = Timestamp.valueOf("1950-01-01 00:00:00")

  def writeDataframe(
    target: TargetSettings.Scylla,
    renames: List[Rename],
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

    // Similarly to createDataFrame, when using withColumnRenamed, Spark tries
    // to re-encode the dataset. Instead we just use the modified schema from this
    // DataFrame; the access to the rows is positional anyway and the field names
    // are only used to construct the columns part of the INSERT statement.
    val renamedSchema = renames
      .foldLeft(df) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }
      .schema

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

    // you have to use --conf spark.months=8 \
    // to trigger only 8 months back data, by default 12 will be used

    log.info("Months to be kept: " + spark.conf.get("spark.months"))
    val checkTS = if (Integer.valueOf(spark.conf.get("spark.months")) == 8) emonths else tmonths

    val frdd = rdd.filter { row =>
//      val keyindex = row.fieldIndex("ts")
      val tsoption = row.get(1).asInstanceOf[CassandraOption[Timestamp]]
      val ts = tsoption.getOrElse(dummyTS)
//      log.info("Timestamp: " + ts)
      ts.asInstanceOf[Timestamp].after(checkTS)
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
