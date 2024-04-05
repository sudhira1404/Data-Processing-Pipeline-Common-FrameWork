package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object DataHandler extends LoggingTrait {

  /**
    * Extract Unique and Duplicate data for a given source Data Frame . This method uses window function and calculates
    * duplicate and unique based on given partition columns and order columns.  1215
    *
    * @param sourceDF         : Source Data Frame
    * @param partitionColumns : Partition Columns as String, example: partitionColumns = "acct_yr_i,acct_mo_i,acct_mo_wk_i,mdse_dept_ref_i,mdse_clas_ref_i,co_loc_ref_i,tran_type_c"
    * @param orderColumns     : Order columns as String , example: orderColumns     = "wk_end_d:desc,timestamp:asc"
    *                         Make sure that columns are separated by comma
    *                         each of column is formatted with column name and its order type , separated by ':'
    * @return               : Return Unique and Duplicate Data Frames
    */
  def getUniqueDuplicateData(sourceDF: DataFrame,
                             partitionColumns:String,
                             orderColumns:String): (DataFrame,DataFrame) ={

    val partitionColumnsList = partitionColumns.split(",")
    val orderColumnsList = orderColumns.split(",")

    val orderColumnsFormat = orderColumnsList.map(name => {
      if (name.split(":")(1) == "desc") {
        col(name.split(":")(0)).desc
      } else {
        col(name.split(":")(0)).asc
      }
    })

    val rowNumber = MarketingConstants.COLNAME_ROW_NO
    val rowNumberDF = sourceDF
      .withColumn(s"$rowNumber", row_number()
        .over(Window.partitionBy(partitionColumnsList.head,partitionColumnsList.tail:_*)
          .orderBy(orderColumnsFormat:_*)))

    val uniqueDF = rowNumberDF.where(s"$rowNumber = 1").drop(s"$rowNumber")
    val duplicateDF = rowNumberDF.where(s"$rowNumber <> 1").drop(s"$rowNumber")

    (uniqueDF,duplicateDF)
  }


}
