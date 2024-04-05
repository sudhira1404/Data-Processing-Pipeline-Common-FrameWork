package com.tgt.dse.mdf.common.pipeline.types

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dse.mdf.common.pipeline.service.ReadFromTableAsPrevUnionIncremental
import org.apache.spark.sql.{DataFrame, SparkSession}

/**

Different implementations of building snapshots based on the class ReadFromTableAsPrevUnionIncremental
 */
object SnapShotType extends SnapShot {

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Builds previous dataset by getting column specified in selectColumns from the foundation table
   * for all partitions available in the incremental
   * and unions previous dataset with incremental for the column specified in selectColumns
   * and removes duplicate based on fndKeyColName ordered by FndOrderByColName in desc
   * (Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols)
   * and picks latest by using row_number.
   */
  //do we need fndDFSelectColumns
  override def LoadFndAfterMergingFndWithIncremental(pipeLineConfig: PipeLineConfig,
                                                     incrementalRightDF: DataFrame,
                                                     selectColumns: Seq[String],
                                                     fndPartitionDateColName: String,
                                                     fndKeyColName: Seq[String],
                                                     fndOrderByColName: String)
                                                    (implicit sparkSession: SparkSession): DataFrame = {

    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        selectColumns=selectColumns,
        fndPartitionDateColName=fndPartitionDateColName,
        fndKeyColName=fndKeyColName,
        fndOrderByColName=fndOrderByColName
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)

  }

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

  override def LoadFndAfterMergingFndWithIncremental(pipeLineConfig: PipeLineConfig,
                                                           incrementalRightDF: DataFrame,
                                                           joinCondition:String,
                                                           selectColumns: Seq[String],
                                                           fndPartitionDateColName: String
                                                      )
                                                          (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        joinCondition=joinCondition,
        selectColumns=selectColumns,
        fndPartitionDateColName=fndPartitionDateColName
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)
  }

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Builds previous dataset by getting column specified in selectColumns from the foundation table
   * for all partitions available in the incremental
   * and unions previous dataset with incremental for column specified in selectColumns
   * and does not removes any duplicates
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   */

  override def LoadFndAfterMergingFndWithIncremental(pipeLineConfig: PipeLineConfig,
                                                     incrementalRightDF: DataFrame,
                                                     selectColumns: Seq[String],
                                                     fndPartitionDateColName: String)
                                                    (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        selectColumns=selectColumns,
        fndPartitionDateColName=fndPartitionDateColName
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)
  }

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Builds previous dataset by getting column specified in selectColumns from the Atomic table
   * for all partitions available in the incremental
   * and joins with incremental and gets records in previous not in incremental based on join condition and
   * unions with incremental for column specified in selectColumns
   */

  override def LoadAtomicAfterMergingAtomicWithIncremental(pipeLineConfig: PipeLineConfig,
                                                           incrementalRightDF: DataFrame,
                                                           joinCondition:String,
                                                           selectColumns: Seq[String],
                                                           fndPartitionDateColName: String,
                                                           atomicTableName:String)
                                                          (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        joinCondition=joinCondition,
        selectColumns=selectColumns,
        fndPartitionDateColName=fndPartitionDateColName,
        atomicTableName=atomicTableName
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)
  }

  //add based on inputhivequery
  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is less than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * (include the fnd partition column in fndKeyColName do remove dup in the incremental by fnd partition column)
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * and builds snapshot by propagating records from earlier partitions to the next
   * and removes dup based on snapShotFndOrderByColName and snapShotFndOrderByColName
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * and value for snapShotWindowingStrategy needs to provided like row_number/dense_rank/rank
   * (include the fnd partition column in fndKeyColName do remove dup after building snapshot by fnd partition column )
   * and if there are gaps in partitions then builds that from previous partitions.
   *
   * If use case is to consider record in the latest partition as latest when compared to the previous partition based on key
   * like in incremental if we get 3 days of data which needs to be loaded to fnd table  by building snapshot
   * Incremental Dataset
   * sap_project_id  milestone name   report_d
   * 1               aaa             2023-01-01
   * 2               bbb             2023-01-01
   * 1               bbb             2023-01-02
   * 1               ccc             2023-01-03

   *Fnd will be
   * sap_project_id  milestone name   report_d
   * 1               aaa             2023-01-01
   * 2               bbb             2023-01-01

   * 1               bbb             2023-01-02
   * 2               bbb             2023-01-02

   * 1               ccc             2023-01-03
   * 2               bbb             2023-01-03


   * PK in the table is "sap_project_id" and partition column is "report_d" then
   * snapShotFndKeyColName will be Seq("sap_project_id","report_d") then snapShotFndOrderByColName  will be "right_report_d"
   * (add "right_" to the partition column) and  snapShotWindowingStrategy should be  "dense_rank"

   * This will compare or left joins, Incremental Dataset(left dataset) with distinct dates of incremental(right dataset)
   * and renames right dataset partition column to "right_" partition column name and builds snapshot
   */
  override def buildSnapShot(pipeLineConfig: PipeLineConfig,
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

                            )
                            (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        selectColumns=selectColumns,
        fndPartitionDateColName = fndPartitionDateColName,
        fndKeyColName = fndKeyColName,
        fndOrderByColName = fndOrderByColName,
        snapShot_F = snapShot_F,
        snapShotFndKeyColName = snapShotFndKeyColName,
        snapShotFndOrderByColName = snapShotFndOrderByColName,
        snapShotWindowingStrategy = snapShotWindowingStrategy,
        snapShotHandleMissingDates = snapShotHandleMissingDates,
        snapShotDebugDFToLog = snapShotDebugDFToLog,
        fndSelectColumns = fndSelectColumns,
        tableIsExternal = tableIsExternal
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)

  }

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is less than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   */
  override def buildSnapShot(pipeLineConfig: PipeLineConfig,
                             incrementalRightDF: DataFrame,
                             selectColumns: Seq[String],
                             fndPartitionDateColName: String,
                             fndKeyColName:Seq[String],
                             fndOrderByColName:String

                            )
                            (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        selectColumns=selectColumns,
        fndPartitionDateColName = fndPartitionDateColName,
        fndKeyColName = fndKeyColName,
        fndOrderByColName = fndOrderByColName
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)

  }

  /**
   * (Same as 1st buildSnapShot  except that snapShotWindowingStrategy is defaulted to row_number)
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is less than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * (include the fnd partition column in fndKeyColName do remove dup in the incremental by fnd partition column)
   * and builds snapshot by propagating records from earlier partitions to the next
   * and removes dup based on snapShotFndOrderByColName and snapShotFndOrderByColName
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * based on default row_number for snapShotWindowingStrategy
   * (include the fnd partition column in fndKeyColName do remove dup after building snapshot by fnd partition column )
   * and if there are gaps in partitions then builds that from previous partitions
   * Incremental Dataset
   * sap_project_id  milestone name   report_d
   * 1               aaa             2023-01-01
   * 2               bbb             2023-01-01
   * 1               bbb             2023-01-02
   * 1               ccc             2023-01-03

   *Fnd will be
   * sap_project_id  milestone name   report_d
   * 1               aaa             2023-01-01
   * 2               bbb             2023-01-01

   * 1               aaa             2023-01-02
   * 1               bbb             2023-01-02
   * 2               bbb             2023-01-02

   * 1               aaa             2023-01-03
   * 1               bbb             2023-01-03
   * 2               bbb             2023-01-03
   * 1               ccc             2023-01-03
   *
   * PK in the table is "sap_project_id" and partition column is "report_d" then
   * snapShotFndKeyColName will be Seq("sap_project_id","report_d") then snapShotFndOrderByColName "report_d"
   * and  snapShotWindowingStrategy will be defaulted to  "row_number"
   */
  override def buildSnapShot(pipeLineConfig: PipeLineConfig,
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

                            )
                            (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        selectColumns=selectColumns,
        fndPartitionDateColName = fndPartitionDateColName,
        fndKeyColName = fndKeyColName,
        fndOrderByColName = fndOrderByColName,
        snapShot_F = snapShot_F,
        snapShotFndKeyColName = snapShotFndKeyColName,
        snapShotFndOrderByColName = snapShotFndOrderByColName,
        snapShotHandleMissingDates = snapShotHandleMissingDates,
        snapShotDebugDFToLog = snapShotDebugDFToLog,
        fndSelectColumns = fndSelectColumns,
        tableIsExternal = tableIsExternal
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)

  }

  /**
   * Refer ReadFromTableAsPrevUnionIncremental for parameter description
   * Gets min date available in incrementalRightDF and
   * gets latest partition in fnd table which is greater than or equal to this min date
   * and unions with incremental and removes dup based on fndKeyColName and fndOrderByColName
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * (include the fnd partition column in fndKeyColName do remove dup in the incremental by fnd partition column)
   * and builds snapshot by propagating records from earlier partitions to the next
   * and removes dup based on snapShotFndOrderByColName and snapShotFndOrderByColName
   * Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * and value for snapShotWindowingStrategy needs to provided like row_number/dense_rank/rank
   * (include the fnd partition column in fndKeyColName do remove dup after building snapshot by fnd partition column )
   * and if there are gaps in partitions then builds that from previous partitions
   * Helpful if there are older partitions in the ephemeral(incremental run) when compared to max partitions in the fnd table
   * which needs to be build
   */
  override def buildSnapShot(pipeLineConfig: PipeLineConfig,
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
                            (implicit sparkSession: SparkSession): DataFrame = {
    val initReadFromTableAsPrevUnionIncremental: ReadFromTableAsPrevUnionIncremental =
      ReadFromTableAsPrevUnionIncremental (
        pipeLineConfig=pipeLineConfig,
        incrementalRightDF=incrementalRightDF,
        selectColumns=selectColumns,
        fndPartitionDateColName = fndPartitionDateColName,
        fndKeyColName = fndKeyColName,
        fndOrderByColName = fndOrderByColName,
        snapShot_F = snapShot_F,
        adhocSnapShotBuildFromEarliestPartitionInFND_F = adhocSnapShotBuildFromEarliestPartitionInFND_F,
        snapShotFndKeyColName = snapShotFndKeyColName,
        snapShotFndOrderByColName = snapShotFndOrderByColName,
        snapShotWindowingStrategy = snapShotWindowingStrategy,
        snapShotHandleMissingDates = snapShotHandleMissingDates,
        snapShotDebugDFToLog = snapShotDebugDFToLog,
        fndSelectColumns = fndSelectColumns,
        tableIsExternal = tableIsExternal
      )


    ReadFromTableAsPrevUnionIncremental.readFromTableAsPrevJoinIncremental(initReadFromTableAsPrevUnionIncremental)

  }



}










