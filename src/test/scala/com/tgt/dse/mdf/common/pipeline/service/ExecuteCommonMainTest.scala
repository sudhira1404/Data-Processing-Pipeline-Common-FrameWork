package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.exceptions.LockFileException
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.exceptions.{ContractValidationException, DataQualityException, FileCopyException, FileMoveException, SchemaException}
import com.tgt.dse.mdf.common.pipeline.service.ExecuteCommonMain.exceptionHandler
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class ExecuteCommonMainTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

  "The ExecuteCommonMainTest object" should "do the following"

  it should "handle Lock File exceptions" in {
    val exception = LockFileException("this is a test of handling a LockFileException")
    exceptionHandler(exception,configTest)
  }

  it should "handle Data Quality exceptions" in {
    val exception = DataQualityException("this is a test of handling a DataQualityException")
    exceptionHandler(exception,configTest)
  }

  it should "handle File Move exceptions" in {
    val exception = FileMoveException("this is a test of handling a FileMoveException")
    exceptionHandler(exception,configTest)
  }

  it should "handle File Copy exceptions" in {
    val exception = FileCopyException("this is a test of handling a FileCopyException")
    exceptionHandler(exception,configTest)
  }

  it should "handle Schema exceptions" in {
    val exception = SchemaException("this is a test of handling a SchemaException")
    exceptionHandler(exception,configTest)
  }

  it should "handle Contract Validation exceptions" in {
    val exception = ContractValidationException("this is a test of handling a ContractValidationException")
    exceptionHandler(exception,configTest)
  }

  it should "handle general exceptions" in {
    val exception = new NullPointerException("this is a test of handling a NullPointerException")
    exceptionHandler(exception,configTest)
  }

}
