package com.tgt.dse.mdf.common.pipeline.types

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SnapShot {

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Builds previous dataset by getting column specified in selectColumns from the foundation table
   * for all partitions available in the incremental
   * and unions previous dataset with incremental for the column specified in selectColumns
   * and removes duplicate based on fndKeyColName ordered by FndOrderByColName in desc and picks latest by using row_number.
   */

  def LoadFndAfterMergingFndWithIncremental(pipeLineConfig: PipeLineConfig,
                                            incrementalRightDF: DataFrame,
                                            selectColumns: Seq[String],
                                            fndPartitionDateColName:String ,
                                            fndKeyColName:Seq[String],
                                            fndOrderByColName:String) (implicit sparkSession: SparkSession):DataFrame


  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Same as before except it joins previous dataset and incremental to identify records in previous not in incremental and then unions incremental
   * (so no remove dups after union)
   * Builds previous dataset by getting column specified in selectColumns from the FND table
   * for all partitions available in the incremental
   * and joins with incremental and gets records in previous not in incremental based on join condition and
   * unions with incremental for column specified in selectColumns
   * If join column name is same in left(previous from fnd table) and right DF(incremental DF) then
   * right DF columns should be prefixed with right_ like
   * val joinCondition = "id = right_id and date = right_date and group_id=campaign_id" and gets all records
   * from previous not in incremental and then union previous and incremental dataset
   */

  def LoadFndAfterMergingFndWithIncremental(pipeLineConfig: PipeLineConfig,
                                            incrementalRightDF: DataFrame,
                                            joinCondition:String,
                                            selectColumns: Seq[String],
                                            fndPartitionDateColName:String) (implicit sparkSession: SparkSession):DataFrame



  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Same as Previous except this doe not remove any duplicates
   * Builds previous dataset by getting column specified in selectColumns from the foundation table
   * for all partitions available in the incremental
   * and unions previous dataset with incremental for column specified in selectColumns
   * and does not removes any duplicates
   */


  def LoadFndAfterMergingFndWithIncremental(pipeLineConfig: PipeLineConfig,
                                            incrementalRightDF: DataFrame,
                                            selectColumns: Seq[String],
                                            fndPartitionDateColName:String) (implicit sparkSession: SparkSession):DataFrame
  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Builds previous dataset by getting column specified in selectColumns from the Atomic table
   * for all partitions available in the incremental
   * and joins with incremental and gets records in previous not in incremental based on join condition and
   * unions with incremental for column specified in selectColumns
   */

  def LoadAtomicAfterMergingAtomicWithIncremental(pipeLineConfig: PipeLineConfig,
                                                  incrementalRightDF: DataFrame,
                                                  joinCondition:String,
                                                  selectColumns: Seq[String],
                                                  fndPartitionDateColName:String,
                                                  atomicTableName:String) (implicit sparkSession: SparkSession):DataFrame
  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is less than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * (include the fnd partition column in fndKeyColName do remove dup in the incremental by fnd partition column)
   * and builds snapshot by propagating records from earlier partitions to the next
   * and removes dup based on snapShotFndOrderByColName and snapShotFndOrderByColName
   * and value for snapShotWindowingStrategy needs to provided like row_number/dense_rank/rank
   * (include the fnd partition column in fndKeyColName do remove dup after building snapshot by fnd partition column )
   * and if there are gaps in partitions then builds that from previous partitions
   */

  def buildSnapShot(pipeLineConfig: PipeLineConfig,
                    incrementalRightDF: DataFrame,
                    selectColumns: Seq[String],
                    fndPartitionDateColName: String,
                    fndKeyColName:Seq[String],
                    fndOrderByColName:String,
                    fndSelectColumns:String,
                    snapShot_F:Boolean,
                    snapShotFndKeyColName:Seq[String],
                    snapShotFndOrderByColName:String,
                    snapShotWindowingStrategy:String,
                    snapShotHandleMissingDates:Boolean,
                    snapShotDebugDFToLog:Boolean,
                    tableIsExternal:Boolean
                   )(implicit sparkSession: SparkSession):DataFrame

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is less than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   */

  def buildSnapShot(pipeLineConfig: PipeLineConfig,
                    incrementalRightDF: DataFrame,
                    selectColumns: Seq[String],
                    fndPartitionDateColName: String,
                    fndKeyColName:Seq[String],
                    fndOrderByColName:String
                   )(implicit sparkSession: SparkSession):DataFrame

  /**
   * (Same as 1st buildSnapShot  except that snapShotWindowingStrategy is defaulted to row_number)
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is less than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * (include the fnd partition column in fndKeyColName do remove dup in the incremental by fnd partition column)
   * and builds snapshot by propagating records from earlier partitions to the next
   * and removes dup based on snapShotFndOrderByColName and snapShotFndOrderByColName
   * based on default row_number for snapShotWindowingStrategy
   * (include the fnd partition column in fndKeyColName do remove dup after building snapshot by fnd partition column )
   * and if there are gaps in partitions then builds that from previous partitions
   */

  def buildSnapShot(pipeLineConfig: PipeLineConfig,
                    incrementalRightDF: DataFrame,
                    selectColumns: Seq[String],
                    fndPartitionDateColName: String,
                    fndKeyColName:Seq[String],
                    fndOrderByColName:String,
                    fndSelectColumns:String,
                    snapShot_F:Boolean,
                    snapShotFndKeyColName:Seq[String],
                    snapShotFndOrderByColName:String,
                    snapShotHandleMissingDates:Boolean,
                    snapShotDebugDFToLog:Boolean,
                    tableIsExternal:Boolean
                   )(implicit sparkSession: SparkSession):DataFrame

  /**
   * (Same as first buildSnapShot except that adhocSnapShotBuildFromEarliestPartitionInFND_F is provided as parameter)
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is greater than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * (include the fnd partition column in fndKeyColName do remove dup in the incremental by fnd partition column)
   * and builds snapshot by propagating records from earlier partitions to the next
   * and removes dup based on snapShotFndOrderByColName and snapShotFndOrderByColName
   * and value for snapShotWindowingStrategy needs to provided like row_number/dense_rank/rank
   * (include the fnd partition column in fndKeyColName do remove dup after building snapshot by fnd partition column )
   * and if there are gaps in partitions then builds that from previous partitions
   * Helpful if there are older partitions in the ephemeral(incremental run) when compared to max partitions in the fnd table
   * which needs to be build
   */

  def buildSnapShot(pipeLineConfig: PipeLineConfig,
                    incrementalRightDF: DataFrame,
                    selectColumns: Seq[String],
                    fndPartitionDateColName: String,
                    fndKeyColName:Seq[String],
                    fndOrderByColName:String,
                    fndSelectColumns:String,
                    snapShot_F:Boolean,
                    adhocSnapShotBuildFromEarliestPartitionInFND_F:Boolean,
                    snapShotFndKeyColName:Seq[String],
                    snapShotFndOrderByColName:String,
                    snapShotWindowingStrategy:String,
                    snapShotHandleMissingDates:Boolean,
                    snapShotDebugDFToLog:Boolean,
                    tableIsExternal:Boolean

                   )
                   (implicit sparkSession: SparkSession): DataFrame


}
