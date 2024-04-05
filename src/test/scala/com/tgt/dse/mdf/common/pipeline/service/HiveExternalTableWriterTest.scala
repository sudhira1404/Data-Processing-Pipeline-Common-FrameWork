package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.constants.CoreConstants
import com.tgt.dsc.kelsa.datapipeline.core.util.FileServiceHelper
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.{Failure, Success, Try}

class HiveExternalTableWriterTest extends AnyFlatSpec with MarketingTestHelpers {

  it should "Write to partitioned location" in {
    Try(FileWriterService.emptyDir("src/test/data/partition_write_test/")(spark))
    match {
      case Success(_) => log.info("Successfully deleted prior test data")
      case Failure(e) => log.error(e.getMessage)
    }
    val ephemeralPath = "src/test/data/partition_write_test/ephemeral/"
    val testPartFile: String = "src/test/resources/load_date=202208160704"
    val testPartFile2: String = "src/test/resources/load_date=202208140701"
    FileWriterService.makeDir(ephemeralPath)
    FileWriterService.copyFile(testPartFile, ephemeralPath)
    FileWriterService.copyFile(testPartFile2, ephemeralPath)

    val externalTablePath = "src/test/data/partition_write_test/external_table"
    val externalAtomicPath = "src/test/data/partition_write_test/external_table_e"
    FileWriterService.makeDir(externalTablePath)
    FileWriterService.makeDir(externalAtomicPath)

    val inputDF: DataFrame = spark.read.json(ephemeralPath).withColumn("report_d", to_date(col("timestamp")))
    inputDF.printSchema()
    val foundationWriter: HiveExternalTableWriter = LocalExternalFoundationTable("stg_sgn_fnd", "some_table",
      Seq("value","load_date","report_d"), externalTablePath).writer
    HiveExternalTableWriter.writeToExternalFndTable(inputDF, foundationWriter)
    val writtenPartitions = FileServiceHelper.retrievePartitions(externalTablePath)
    assert((writtenPartitions.contains(List("report_d=2022-08-13"))))
    assert((writtenPartitions.contains(List("report_d=2022-08-15"))))
    assert((writtenPartitions.contains(List("report_d=2022-08-16"))))

    val atomicWriter: HiveExternalTableWriter = LocalExternalAtomicTable("stg_sgn_lzn", "some_table_e",
      Seq("key","offset","partition","timestamp","timestampType","topic","value","load_date","report_d"),externalAtomicPath).writer
    HiveExternalTableWriter.writeToExternalFndTable(inputDF, atomicWriter)
    val writtenAtomicPartitions = FileServiceHelper.retrievePartitions(externalAtomicPath)
    assert((writtenAtomicPartitions.contains(List("report_d=2022-08-13"))))
    assert((writtenAtomicPartitions.contains(List("report_d=2022-08-15"))))
    assert((writtenAtomicPartitions.contains(List("report_d=2022-08-16"))))
  }

  it should "Initialize an External Foundation Table writer" in {
    val db = "prd_mdf_fnd"
    val tbl = "some_table_name"
    val columns = Seq("column1", "column2")
    val env = "prd"
    val viewName = HiveExternalTableWriter.generateViewName(env, db, tbl)
    val sqlStatement = HiveExternalTableWriter.generateSqlStatement(columns, viewName, MarketingConstants.DEFAULT_PARTITION_COLUMNS,
      CoreConstants.OVERWRITE, db, tbl)


    val writer: HiveExternalTableWriter = HiveExternalFoundationTable(
      env = env,
      schemaName = db,
      tableName = tbl,
      insertColumnNames = columns
      ).writer

    assertResult(CoreConstants.OVERWRITE)(writer.writeMode)
    assertResult(env)(writer.env)
    assertResult(columns)(writer.insertColumnNames)
    assertResult(db)(writer.schema)
    assertResult(tbl)(writer.tableName)
    assertResult(MarketingConstants.DEFAULT_PARTITION_COLUMNS)(writer.partitionColumnNames)
    assertResult(MarketingConstants.WRITE_RETRY_COUNT)(writer.writeRetryCount)
    assertResult(MarketingConstants.WRITE_RETRY_SLEEP)(writer.writeRetrySleep)
    assertResult(sqlStatement)(writer.sqlStatement)
    assertResult(viewName)(writer.viewName)


  }

  it should "Initialize an External Atomic Table writer" in {
    val db = "prd_mdf_lzn"
    val tbl = "some_table_name"
    val columns = Seq("column1", "column2")
    val env = "prd"
    val viewName = HiveExternalTableWriter.generateViewName(env, db, tbl)
    val sqlStatement = HiveExternalTableWriter.generateSqlStatement(columns,viewName,MarketingConstants.DEFAULT_PARTITION_COLUMNS,
      CoreConstants.OVERWRITE, db, tbl)

    val writer: HiveExternalTableWriter = HiveExternalAtomicTable(
      env = env,
      schemaName = db,
      tableName = tbl,
      insertColumnNames = columns
    ).writer

    assertResult(CoreConstants.OVERWRITE)(writer.writeMode)
    assertResult(env)(writer.env)
    assertResult(columns)(writer.insertColumnNames)
    assertResult(db)(writer.schema)
    assertResult(tbl)(writer.tableName)
    assertResult(MarketingConstants.DEFAULT_PARTITION_COLUMNS)(writer.partitionColumnNames)
    assertResult(MarketingConstants.WRITE_RETRY_COUNT)(writer.writeRetryCount)
    assertResult(MarketingConstants.WRITE_RETRY_SLEEP)(writer.writeRetrySleep)
    assertResult(sqlStatement)(writer.sqlStatement)
    assertResult(viewName)(viewName)


  }

  it should "Initialize a Local Directory writer" in {
    val db = "prd_mdf_lzn"
    val tbl = "some_table_name"
    val columns = Seq("column1", "column2")
    val env = "prd"
    val loc = "file:///some/dir/"
    val viewName = HiveExternalTableWriter.generateViewName(env,db,tbl,loc)
    val sqlStatement = HiveExternalTableWriter.generateSqlStatement(viewName = viewName,allColumns = columns, dbName = db,tableName = tbl,writeToDir = loc,
      localDirWrite = true)

    val writer: HiveExternalTableWriter = LocalExternalDirectory(
      env = env,
      sourceDb = db,
      sourceTable = tbl,
      selectColumnNames = columns,
      writeToDir = loc
    ).writer

    assertResult(env)(writer.env)
    assertResult(columns)(writer.insertColumnNames)
    assertResult(db)(writer.schema)
    assertResult(tbl)(writer.tableName)
    assertResult(MarketingConstants.WRITE_RETRY_COUNT)(writer.writeRetryCount)
    assertResult(MarketingConstants.WRITE_RETRY_SLEEP)(writer.writeRetrySleep)
    assertResult(sqlStatement)(writer.sqlStatement)
    assertResult(viewName)(writer.viewName)

  }

  it should "Initialize an HDFS Directory writer" in {
    val db = "prd_mdf_lzn"
    val tbl = "some_table_name"
    val columns = Seq("column1", "column2")
    val env = "prd"
    val loc = "hdfs:///some/dir/"
    val viewName = HiveExternalTableWriter.generateViewName(env,db,tbl,loc)
    val sqlStatement = HiveExternalTableWriter.generateSqlStatement(viewName = viewName, allColumns = columns, dbName = db, tableName = tbl,
      writeToDir = loc)

    val writer: HiveExternalTableWriter = HdfsExternalDirectory(
      env = env,
      sourceDb = db,
      sourceTable = tbl,
      selectColumnNames = columns,
      writeToDir = loc
    ).writer

    assertResult(env)(writer.env)
    assertResult(columns)(writer.insertColumnNames)
    assertResult(db)(writer.schema)
    assertResult(tbl)(writer.tableName)
    assertResult(MarketingConstants.WRITE_RETRY_COUNT)(writer.writeRetryCount)
    assertResult(MarketingConstants.WRITE_RETRY_SLEEP)(writer.writeRetrySleep)
    assertResult(sqlStatement)(writer.sqlStatement)
    assertResult(viewName)(writer.viewName)

  }
}
