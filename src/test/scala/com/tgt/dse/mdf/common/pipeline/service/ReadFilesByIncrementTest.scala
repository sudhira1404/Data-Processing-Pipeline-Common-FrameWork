package com.tgt.dse.mdf.common.pipeline.service

  import com.tgt.dse.mdf.common.pipeline.service.ReadFilesByIncrement.readFilesByIncrement
  import com.tgt.dse.mdf.common.pipeline.{MarketingPipelineConfigs, MarketingTestHelpers}
  import org.scalatest.flatspec.AnyFlatSpec


  class ReadFilesByIncrementTest extends AnyFlatSpec with MarketingTestHelpers {

    //for testing readFilesByIncrement
    FileWriterService.emptyDir("src/test/data/readfilesbyincrementstartdate")(spark)
    FileWriterService.emptyDir("src/test/data/readfilesbyincrementstartdatearchive")(spark)
    FileWriterService.emptyDir("src/test/data/readfilesbyincrementstartdatearchivemaxdate")(spark)
    FileWriterService.emptyDir("src/test/data/readfilesbyincrement/files")(spark)
    FileWriterService.emptyDir("src/test/data/readfilesbyincrement/archive")(spark)
    FileWriterService.deleteHiddenFile("src/test/resources/readfilesbyincrement/archive")
    FileWriterService.deleteHiddenFile("src/test/resources/readfilesbyincrement/files")
    FileWriterService.makeDir("src/test/data/readfilesbyincrementstartdate")
    FileWriterService.makeDir("src/test/data/readfilesbyincrementstartdatearchive")
    FileWriterService.makeDir("src/test/data/readfilesbyincrementstartdatearchivemaxdate")
    FileWriterService.makeDir("src/test/data/readfilesbyincrement/files")
    FileWriterService.makeDir("src/test/data/readfilesbyincrement/archive")
    FileWriterService.copyFile("src/test/resources/readfilesbyincrementstartdate", "src/test/data/readfilesbyincrementstartdate")
    FileWriterService.copyFile("src/test/resources/readfilesbyincrementstartdatearchive", "src/test/data/readfilesbyincrementstartdatearchive")
    FileWriterService.copyFile("src/test/resources/readfilesbyincrementstartdatearchivemaxdate", "src/test/data/readfilesbyincrementstartdatearchivemaxdate")


    it should "Should read files with date in the filename by increment starting from the date greater" +
      " than in the StartDateFile and copies to ephemeral path " in {

      val listOfFiles = readFilesByIncrement((config: MarketingPipelineConfigs) => Unit,
        sftpTestMarketingPipelineConfig,
        "src/test/resources/readfilesbyincrement/files",
        "src/test/data/readfilesbyincrement/files",
        increment = 1,
        "yyyy-MM-dd",
        "src/test/data/readfilesbyincrementstartdate",
        isHdfsFileSystem=false,
        //maxEndDate = historyMaxEndDate,
        readDateFromFileElseArchive = true)

      assert(listOfFiles.size.toString matches "3")
    }

    it should "Should read files from partition folder by increment starting from the date greater" +
      " than in the StartDateFile and copies to ephemeral path " in {

      val listOfFiles = readFilesByIncrement((config: MarketingPipelineConfigs) => Unit,
        sftpTestMarketingPipelineConfig,
        "src/test/resources/readfilesbyincrement/archive",
        "src/test/data/readfilesbyincrement/archive",
        increment = 1,
        "yyyy-MM-dd",
        "src/test/data/readfilesbyincrementstartdatearchive",
        isHdfsFileSystem=false,
        //maxEndDate = historyMaxEndDate,
        readDateFromFileElseArchive = false)
      assert(listOfFiles.size.toString matches "3")
    }

    it should "Should read files from partition folder by increment starting from the date greater" +
      " than in the StartDateFile till maxEndDate and copies to ephemeral path " in {

      val listOfFiles = readFilesByIncrement((config: MarketingPipelineConfigs) => Unit,
        sftpTestMarketingPipelineConfig,
        "src/test/resources/readfilesbyincrement/archive",
        "src/test/data/readfilesbyincrement/archive",
        increment = 1,
        "yyyy-MM-dd",
        "src/test/data/readfilesbyincrementstartdatearchivemaxdate",
        isHdfsFileSystem=false,
        maxEndDate = "2023-03-21",
        readDateFromFileElseArchive = false)

      assert(listOfFiles.size.toString matches "2")
    }

  }
