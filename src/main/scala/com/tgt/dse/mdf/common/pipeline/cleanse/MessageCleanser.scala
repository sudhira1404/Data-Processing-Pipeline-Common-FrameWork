package com.tgt.dse.mdf.common.pipeline.cleanse

import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.util.DPPMetricUtils
import org.apache.spark.sql.DataFrame

/**
 * All cleanse steps will go here
 */
object MessageCleanser extends LoggingTrait {

  /**
   * Remove duplicates possibly created by explode or the same message coming again and again
   * @param inputDF input dataframe
   * @return Dataframe without duplicates
   */


  def removeDuplicates(inputDF: DataFrame): DataFrame = {
    PipeLineMetadataService.setStringMetricValue( MarketingConstants.STAGE, MarketingConstants.REMOVEDUPLICATES)

    val initialCount: Long = inputDF.count()
    logInfo("Initial count: " + initialCount)

    //removing row level duplicates
    val distinctDF = inputDF.distinct()
    val distinctCount :Long = distinctDF.count()
    logInfo("Distinct count: " + distinctCount)
    DPPMetricUtils.setTransformationRowsCount(distinctCount)

    //calculate number of duplicate rows.
    val difference : Long = initialCount - distinctCount
    logInfo("Number of duplicated rows: " + difference)

    //update duplicate rows metric
    DPPMetricUtils.setDedupRowsCount(difference)
    distinctDF
  }

}
