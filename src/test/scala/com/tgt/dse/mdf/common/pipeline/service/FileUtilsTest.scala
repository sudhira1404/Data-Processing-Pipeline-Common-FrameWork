package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.{getFileNameAndExtensionFromFileName, getFilePathFileNameExtensionFromFilePath, getValue, shellCmdRun}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class FileUtilsTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

    it should "Should get filename and extension from filename" in {
      val getFileNameAndExtension = getFileNameAndExtensionFromFileName("src/test/data/test.csv")
      assertResult(getFileNameAndExtension)(("test","csv"))
    }


  it should "Should get path,filename  extension from filepath" in {
    val getPathFileNameExtension = getFilePathFileNameExtensionFromFilePath("src/test/data/test.csv")
    assertResult(getPathFileNameExtension)(("src/test/data/","test.csv","csv"))
  }

  it should "Should get value as option" in {
    val getValueOption = getValue(Option("src/test/data/test.csv"))
    assertResult(getValueOption)("src/test/data/test.csv")
  }

  it should "Should get execute Shell Cmd" in {
    val executeShellCmd = shellCmdRun("ls")
    assertResult(executeShellCmd)(0)
  }

}
