package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService.{deleteHiddenFile, hdfsMove, readDynamicJson}
import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.util.{Failure, Try}

class FileWriterServiceTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

  "The FileWriterUtilTest object" should "do the following"

  it should "return true if files are moved " in {

    makeOrEmptyDir(inputEphemeralPath)

    val tot_filesize = FileWriterService.hdfsMove(FileWriterService(
      testFacebookJsonFile,
      inputEphemeralPath,
      zerobyteabort = true,
      deletesource = false))(spark)
    assert(tot_filesize.>(0))
  }

  it should "return true if files size is zero " in {
    makeOrEmptyDir(archivePath)
    val result = Try(FileWriterService.hdfsMove(FileWriterService(archivePath
      ,archivePath,
      zerobyteabort = true,
      deletesource = false)))

    match {
      case Failure(_) => true
      case _ => false
    }
    print(result)
    assertResult(true)(result)
  }

  it should "parse dynamic json and produce a csv file in hdfs and throw a count " in {
    makeOrEmptyDir(archivePathPartition)
    val totFilesize = hdfsMove(FileWriterService(
      srcpath=testPinterestJsonFile,
      destpath=archivePathPartition,
      tablename="pinterest_api_actuals_e",
      deletesource = false,
      dynamicJsonToCsv=true,
      zerobyteabort=false))(spark)
    assert(totFilesize.>(0))

  }

  it should "test dynamic json file read" in {
    val count = 2
    val testPinterestJsonFile: String = "src/test/resources/pinterest-report-2021-11-08_2021-11-08.json"
    val inputPinterestDF: DataFrame = readDynamicJson(testPinterestJsonFile)
    assertResult(count)(inputPinterestDF.count())
  }

//  it should "test dynamic json multiple file read" in {
//
//    val destFilepath="src/test/resources/data"
//    val srcFilepath="src/test/resources/pinterest-report-2021-11-08_2021-11-08.json"
//    makeOrEmptyDir(destFilepath)
//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    fs.copyFromLocalFile(new Path(srcFilepath),new Path(destFilepath))
//    val count = 2
//    val inputPinterestDF: DataFrame = readDynamicJsonMultipleFiles(destFilepath)
//    assertResult(count)(inputPinterestDF.count())
//  }

  it should "untar" in {
    val tarGzFile = "src/test/resources/input_data/"
    //val tarGzFile = "src/test/resources/gam_api_forecasts-report-2021-11-30_2021_11-30.json.tar.gz"
    val outPath = "src/test/data/data-processing-pipeline/land/"
    makeOrEmptyDir(outPath)
    FileWriterService.hdfsMove(FileWriterService(srcpath = tarGzFile,
      destpath = outPath,
      zerobyteabort = true,
      deletesource = false))
  }

  it should "measure the size of the directory" in {
    val outPath = "src/test/resources"
    val size = FileWriterService.hdfsSize(outPath,MarketingConstants.LOCAL_ENV)
    assert(size > 0)
  }

  it should "filter on only the files matching the string" in {
    val dir = "src/test/resources/"
    val fileStatus = FileWriterService.filterFileStatusArray("forecasts",dir)
    val expectedNumFilesMatching: Int = 1
    assertResult(expectedNumFilesMatching)(fileStatus.length)
  }

  it should "move only untar files matching the filter string" in {
    val sourceDir = "src/test/resources"
    val tarGzFile = "forecasts.json.tar.gz"
    val outPath = "src/test/data/data-processing-pipeline/land"
    makeOrEmptyDir(outPath)
    FileWriterService.hdfsMove(FileWriterService(sourceDir,
      outPath,
      zerobyteabort = true,
      deletesource = false,
      filter = tarGzFile))
  }

  it should "should delete any hidden files" in {
    deleteHiddenFile("src/test/resources")
  }
}
