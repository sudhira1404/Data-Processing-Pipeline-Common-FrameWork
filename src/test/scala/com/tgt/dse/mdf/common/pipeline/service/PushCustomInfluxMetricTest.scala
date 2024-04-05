package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.PushCustomInfluxMetric.pushCustomInfluxMetricWrapper
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class PushCustomInfluxMetricTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen{


  it should "Push custom influx metrics" in {

    val bool = pushCustomInfluxMetricWrapper(pipeLineConfig)(spark)

    assertResult(true)(bool)
  }

}
