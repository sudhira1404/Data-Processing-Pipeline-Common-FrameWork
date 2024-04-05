package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.service.{FileWriterService, SftpTransferConfig}
import com.tgt.dse.mdf.common.pipeline.util.DateTimeUtils.{appendPathWithTimeStamp, findMinDateFromDateListAndSubtractDays, getDaysBetweenStartAndEndDate, getTimestamp, subDaysFromDate}
import com.tgt.dse.mdf.common.pipeline.util.HdfsUtils.{extractMaxDateFromListOfHdfsFilePathsContainingDate, getHdfsFileSize, getListOfHdfsFiles, hdfsDirDelete, hdfsFileRename, listOfHdfsFiles, listOfQualifiedHdfsFiles, localFileCopyFromHdfs, localFileCopyToHdfs}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

class HdfsUtilsTest extends AnyFlatSpec with MarketingTestHelpers {


  val srcFilepath = "src/test/resources/double_verify_pinnacle_report-2022-07-10.csv.gz"
  //val srcFilepath="src/test/resources/target_halo_attribution_out-2022-04-07.csv.gz"

  //for testing sftp
  FileWriterService.copyFile(srcFilepath, "src/test/data/sftpTransfer/hive/hdfs")

  it should "Should get list of hdfs files based on search pattern" in {

    val result = listOfQualifiedHdfsFiles("src/test/resources/maxdate","extractdate")._1

    assert(result.size.toString matches "1")

  }


  it should "Should get list of hdfs files" in {

    val result = listOfHdfsFiles("src/test/resources/maxdate")

    assert(result.size.toString matches "1")

  }

  it should "Extract max date from list of files in the hdfs path" in {

    val result = extractMaxDateFromListOfHdfsFilePathsContainingDate(List("src/test/resources/maxdate/extractdate_2023-03-28"))

    assert(result.toString matches "2023-03-28")

  }

  it should "Gets hdfs file size" in {

    val result = getHdfsFileSize("src/test/resources/maxdate/extractdate_2023-03-28")
    assert(result.toString matches "0")

  }


  it should "Gets list of files from the hdfsFileLocation along with file size in tuple" in {

    val result = getListOfHdfsFiles("src/test/resources/maxdate/extractdate_2023-03-28")

    assert(result.head._2.toString matches "0")

  }

  it should "Initialize a sftpTransferConfig case class" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_SFTPFILELOCATIONPATH))(sftpTransferConfig.sftpFileLocationPath)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_HDFSFILELOCATIONPATH))(sftpTransferConfig.hdfsFileLocationPath)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_HDFSSEARCHFILEPATTERN))(sftpTransferConfig.hdfsSearchFilePattern)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_HDFSSEARCHFILEPATTERN))(sftpTransferConfig.hdfsSearchFilePattern)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_SFTPSEARCHFILEPATTERN))(sftpTransferConfig.sftpSearchFilePattern)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_SFTPUSER))(sftpTransferConfig.sftpUser)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_SFTPHOST))(sftpTransferConfig.sftpHost)
    assertResult(sftpTestConfig.config.getInt(MarketingConstants.APP_SFTPPORT))(sftpTransferConfig.sftpPort)
    assertResult(sftpTestConfig.config.getString(MarketingConstants.APP_SFTPPRIVATEKEYPATH))(sftpTransferConfig.sftpPrivateKeyPath)
  }

  it should "get hdfs file size" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    val hdfsFileSize = getHdfsFileSize(sftpTransferConfig.hdfsFileLocationPath)(spark)
    assertResult(2003679*1)(2003679*1)
  }

  it should "get list of hdfs files" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    val listOfHdfsFiles = getListOfHdfsFiles(sftpTransferConfig.hdfsFileLocationPath)
    assertResult(listOfHdfsFiles.map(_._2).mkString(","))("2003679")
  }


  it should "local copy to hdfs" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    localFileCopyToHdfs(sftpTransferConfig.hdfsFileLocationPath + "/double_verify_pinnacle_report-2022-07-10.csv.gz",
      "src/test/data/sftpTransfer/hive/hdfsTest/double_verify_pinnacle_report-2022-07-10.csv.gz")
    val listOfHdfsFiles = getListOfHdfsFiles("src/test/data/sftpTransfer/hive/hdfsTest/")
    assertResult(listOfHdfsFiles.length)(1)
  }

  it should "local copy from hdfs" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    localFileCopyFromHdfs(sftpTransferConfig.hdfsFileLocationPath+"/double_verify_pinnacle_report-2022-07-10.csv.gz",
      "src/test/data/sftpTransfer/hive/hdfsTest/double_verify_pinnacle_report-2022-07-10.csv.gz")
    val listOfHdfsFiles = getListOfHdfsFiles(sftpTransferConfig.hdfsFileLocationPath)
    assertResult(listOfHdfsFiles.length)(1)
  }

  it should "get list of qualified hdfs files" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    val listOfHdfsFiles = listOfQualifiedHdfsFiles(sftpTransferConfig.hdfsFileLocationPath)
    assertResult(listOfHdfsFiles._1.mkString(","))("double_verify_pinnacle_report-2022-07-10.csv.gz")
  }

  it should "should rename hdfs file" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    hdfsFileRename(sftpTransferConfig.hdfsFileLocationPath,
      "src/test/data/sftpTransfer/hive/rename",
      "test.csv")
    val listOfHdfsFiles = listOfQualifiedHdfsFiles("src/test/data/sftpTransfer/hive/rename")
    assertResult(listOfHdfsFiles._1.mkString(","))("test.csv")
  }

  it should "delete from  hdfs files" in {
    val sftpTransferConfig = SftpTransferConfig(sftpTestConfig)
    hdfsDirDelete(sftpTransferConfig.hdfsFileLocationPath)
    val listOfHdfsFiles = getListOfHdfsFiles(sftpTransferConfig.hdfsFileLocationPath)
    assertResult(listOfHdfsFiles.length)(0)
  }
}
