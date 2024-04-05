package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.util.DPPMetricUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType

object RevenueTransformer {

  def convertMicroToCurrency(inputDF: DataFrame,
                             columnList: Seq[String]): DataFrame = {
    val typeCastedDF: DataFrame =
      columnList.foldLeft(inputDF)(
        (tempDF, x) =>
          tempDF.withColumn(x, col(x)
            .cast(new DecimalType(precision = MarketingConstants.DECIMAL_PRECISION,scale = MarketingConstants.DECIMAL_SCALE))
            .divide(MarketingConstants.MICRO_CURRENCY_CONVERSION_DENOMINATOR))
      )
    DPPMetricUtils.setTransformationRowsCount(typeCastedDF.count())
    typeCastedDF
  }
}
