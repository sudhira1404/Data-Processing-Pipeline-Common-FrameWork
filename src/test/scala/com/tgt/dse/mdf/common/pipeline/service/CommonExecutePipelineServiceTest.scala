package com.tgt.dse.mdf.common.pipeline.service


import com.tgt.dse.mdf.common.pipeline.CommonExecutePipeLineHelpers
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class CommonExecutePipelineServiceTest extends AnyFlatSpec with CommonExecutePipeLineHelpers with Matchers with GivenWhenThen with CommonExecutePipelineService {

  "The PipelineServiceTest object" should "do the following"

  it should "Should execute the pipeline" in {

    CommonExecutePipeLineService.executePipeLine(testMarketingPipelineConfig)
  }


}