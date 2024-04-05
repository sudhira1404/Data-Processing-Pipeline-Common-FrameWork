package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.service.FileReader.readFileToDataFrame
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object DmaCodeTransformer {

  /**
   * Gets dma name/code based on zip_code or dma_code join
   * @param inputDF         The initial DataFrame
   * @param dmaPath         path of DMA zip file
   * @param zipFileFormat   file format of zip file
   * @param joinCondition    join condition to join inputDF with zip as a string value.If for eg joining based on dma_code,
   * if dma_code column exists in input as well as zip file then zip file's(right table) dma_code column should be prefixed with "right_"
   * like   val joinConditionDma=  "dma_code = right_dma_code" as the method joinTwoDataFrames renames the right table column to
   * right_dma_code
   * @param zipFileReaderOptions   options as map value to be used while reading the zip file in file reader
   * @param zipCodeLevel  Boolean.Defaulted to true.If true then join condition should be based on zip code else dma_code
   * @param dmaCodeColName  dma code column in the zip file
   * @param dmaNameColName  dma name column in the zip file
   * @return    returns tuple of dma dataframe and renamed common column between the right and left table
   *            (it prefixes  "right_" to the column which are common in right and left table
   *            in the right table)
   *            which could be used to drop
   */

  def decorateWithDmaCode(inputDF: DataFrame,dmaPath:String,zipFileFormat:String,joinCondition:String,
                          zipFileReaderOptions:Map[String,String] = Map[String,String](),
                          zipCodeLevel:Boolean=true,dmaCodeColName:String="",dmaNameColName:String="")
                         (implicit sparkSession: SparkSession): (DataFrame, Array[String]) = {

    val zipDmaDF: DataFrame =
      readFileToDataFrame(
        dmaPath,
        fileFormat = zipFileFormat,
        options = zipFileReaderOptions
      )
    val zipDmaTransformedDF = if (zipCodeLevel)
    {
      zipDmaDF
    }
    else {
      zipDmaDF.select(if(dmaCodeColName.isEmpty) "dma_code" else dmaCodeColName,
        if(dmaNameColName.isEmpty) "dma_name" else dmaNameColName)
        .distinct()
    }
    //zipDmaTransformedDF.show(2,false)
    val joinTwoDataFramesTuple = DataFrameUtils.joinTwoDataFrames(inputDF,zipDmaTransformedDF,"left",joinCondition)
    val codeList = Seq("right_dma_code","right_zip_code","right_dmacode","right_zipcode")
    val joinTwoDataFramesDf = joinTwoDataFramesTuple._1.drop(codeList: _*)
    val joinTwoDataFramesDfDropRightList = joinTwoDataFramesTuple._2
    val joinTwoDataFramesDfDropRightListNew = joinTwoDataFramesDfDropRightList.filterNot(codeList.contains(_) )

    (joinTwoDataFramesDf,joinTwoDataFramesDfDropRightListNew)
  }

}
