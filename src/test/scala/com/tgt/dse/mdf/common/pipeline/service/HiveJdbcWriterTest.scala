package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.types.TableDetails.{FinalManagedTable, IntermediateExternalTable}
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class HiveJdbcWriterTest extends AnyFlatSpec with MarketingTestHelpers {

  it should "Initialize a FileWriter case class" in {
    val fileWriter = HiveJdbcWriter(pipeLineConfig,Seq(FIPS,COUNTY,STATE,DMA_CODE,DMA_NAME,ZIP_CODE),Seq(STATE))
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_coreHistTable))(fileWriter.coreHistTableName)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_FNDSCHEMA))(fileWriter.coreHistSchema)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_QUEUENAME))(fileWriter.appQueueName)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_EXECUTIONENGINE))(fileWriter.appExecutionEngine)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_DYNAMICPARTITION))(fileWriter.appDynamicPartition)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_DYNAMICPARTITIONMODE))(fileWriter.appDynamicPartitionMode)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_JDBCURL))(fileWriter.appJdbcUrl)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_TEMPFNDEXTLOCATION))(fileWriter.tempFndExtLocation)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_fndExtTable))(fileWriter.externalFndTableName)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_WRKSCHEMA))(fileWriter.externalFndDatabaseName)
    assertResult(2)(fileWriter.HDFS_WRITE_RETRY_COUNT)
    assertResult(
      IntermediateExternalTable(fileWriter.externalFndDatabaseName,fileWriter.externalFndTableName,fileWriter.tempFndExtLocation)
    )(fileWriter.externalTableDetails)
    assertResult(
      FinalManagedTable(fileWriter.coreHistSchema,fileWriter.coreHistTableName,fileWriter.foundationPartitionColumnNames)
    )(fileWriter.managedTableDetails)
    assertResult("org.apache.hive.jdbc.HiveDriver")(fileWriter.HIVE_DRIVER_CLASS)
  }

  it should "Arrange column order" in {
    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
      predeterminedSchema = DMAZIPCODETEMPLATESCHEMA)
    log.info("Schema prior to rearranging column order:")
    testCsvDF.printSchema()
    val fileWriter = HiveJdbcWriter(pipeLineConfig,Seq(FIPS,COUNTY,DMA_CODE,DMA_NAME,ZIP_CODE,STATE),Seq(STATE))
    val columnOrder = HiveJdbcWriter.arrangeColumnOrder(testCsvDF, fileWriter)
    log.info(s"Count of rearranged DataFrame: ${columnOrder._2}")
    log.info("Schema from rearranged column order: ")
    columnOrder._1.printSchema()
    assertResult(testDmaZipcodeFileRowCount)(columnOrder._2)
  }

  it should "Generate Hive Properties" in {
    val fileWriter = HiveJdbcWriter(pipeLineConfig,Seq(FIPS,COUNTY,DMA_CODE,DMA_NAME,ZIP_CODE,STATE),Seq(STATE))
    val properties = HiveJdbcWriter.generateHiveProperties(fileWriter)
    assertResult(false)(properties.properties.isEmpty)
    assert(properties.jdbcUrl.nonEmpty)
    assert(properties.driverName.nonEmpty)

  }

  it should "Initialize connection properties" in {
    val connectionProperties = HiveJdbcWriter.setJdbcConnectionProperties("default")
    assertResult("default")(connectionProperties.getProperty("tez.queue.name"))
    assertResult("org.apache.hadoop.hive.ql.io.HiveInputFormat")(connectionProperties.getProperty("hive.tez.input.format"))
    assertResult("50")(connectionProperties.getProperty("mapreduce.job.reduces="))
  }

  it should "Generate a partition string from a list" in {
    val expectedString = "PARTITION(report_d=\"2022-02-02\") PARTITION(report_d=\"2022-02-03\")"
    val listOfPartitions = List(List("report_d=2022-02-02"),List("report_d=2022-02-03"))
    val outcome = HiveJdbcWriter.stringBuilderPartitionList(listOfPartitions)
    log.info(s"outcome : $outcome")
    assertResult(expectedString)(outcome)
  }

}
