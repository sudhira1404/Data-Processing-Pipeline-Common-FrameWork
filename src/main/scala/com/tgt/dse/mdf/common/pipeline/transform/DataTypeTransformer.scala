package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.util.DPPMetricUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

/**
 * Transformation functions for DataTypes
 */
object DataTypeTransformer {

  /**
   * Casts all columns specified into the same DataType in the input DataFrame, producing a new DataFrame
   * @param inputDF The starting DataFrame
   * @param columns The Sequence of column names which are in the inputDF and will be cast to the dataType
   * @param dataType The DataType to cast each of the specified columns to
   * @return A new DataFrame that has each of the columns in the columns sequence cast to the new DataType
   */
  def castToDataType(inputDF: DataFrame, columns: Seq[String], dataType: DataType): DataFrame = {
    val typeCastedDF: DataFrame = {
      columns.foldLeft(inputDF)(
        (tempDF, x) =>
          tempDF.withColumn(x, col(x).cast(dataType))
      )
    }
    DPPMetricUtils.setTransformationRowsCount(typeCastedDF.count())
    typeCastedDF
  }

}
