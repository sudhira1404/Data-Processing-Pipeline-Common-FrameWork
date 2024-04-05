package com.tgt.dse.mdf.common.pipeline.types

import com.tgt.dsc.kelsa.datapipeline.core.constants.CoreConstants
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.service.HiveExternalTableWriter

/**
 * Initialize a [[HiveExternalTableWriter]] class based on the parameters for an External Hive Foundation Table.
 * This case class should NOT be used for local testing, only for stage or production definitions. The output of the
 * companion DataFrame will be written to the specified Hive table rather than the filesystem. Use [[LocalExternalFoundationTable]]
 * for local testing instead. The writer value will be passed to [[HiveExternalTableWriter.writeToExternalFndTable]] like this:
 * {{{
 *   //The finalDataFrame represents the very last DF,
 *   //after passing all DQ checks, transformations, cleansing, etc.
 *   val finalDataFrame: DataFrame
 *
 *   val writer: HiveExternalTableWriter = HiveFoundationTable(
 *     env = "prd",
 *     schemaName = "prd_mdf_fnd",
 *     tableName = "some_table_name",
 *     insertColumnNames = Seq("column1","column2",etc)
 *   ).writer
 *
 *   HiveExternalTableWriter.writeToExternalFndTable(finalDataFrame, writer)
 * }}}
 *
 * @param env The environment of the external Hive table. Should either be "prd" or "stg"
 * @param schemaName The database of the external foundation table. This is usually either "prd_mdf_fnd" or "stg_mdf_fnd"
 * @param tableName The external foundation table name.
 * @param insertColumnNames A sequence of all columns on the external foundation table, in order, including the partition column(s).
 * @param partitionColumns [OPTIONAL] A sequence of only the partition column(s). Defaults to `Seq("report_d")`
 * @param retryCount [OPTIONAL] Sets how many attempts to retry in case there are failures encountered with the insert to Hive external table.
 *                            Defaulted to 3.
 * @param retrySleep [OPTIONAL] Sets how long to sleep in between retry attempts. Defaulted to 3 seconds.
 * @param mode [OPTIONAL] Used to specify the mode either saving to local filesystem or writing to external Hive table. Defaults to "overwrite"
 */
final case class HiveExternalFoundationTable(env: String, schemaName: String, tableName: String, insertColumnNames: Seq[String],
                                             partitionColumns: Seq[String] = MarketingConstants.DEFAULT_PARTITION_COLUMNS,
                                             retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                             retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                                             mode: String = CoreConstants.OVERWRITE,
                                             repartition: Boolean = true, recordFinalCount: Boolean = true
                                     ) extends HiveExternalTable {
  val writer: HiveExternalTableWriter = initWriter(env, insertColumnNames, partitionColumns, schemaName, tableName,
    retryCount = retryCount, retrySleep = retrySleep, mode = mode,repartition = repartition, recordFinalCount = recordFinalCount
  )
}


/**
 * Initialize a [[HiveExternalTableWriter]] class based on the parameters for an External Hive Atomic Table.
 * This case class should NOT be used for local testing, only for stage or production definitions. The output of the
 * companion DataFrame will be written to the specified Hive table rather than the filesystem. Use [[LocalExternalAtomicTable]]
 * for local testing instead. The writer value will be passed to [[HiveExternalTableWriter.writeToExternalFndTable]] like this:
 * {{{
 *   //The initialDataFrame represents the very first DF,
 *   //after reading from file and no further transformations are performed.
 *   val initialDataFrame: DataFrame
 *
 *   val writer: HiveExternalTableWriter = HiveAtomicTable(
 *     env = "prd",
 *     schemaName = "prd_mdf_lzn",
 *     tableName = "some_table_name",
 *     insertColumnNames = Seq("column1","column2",etc)
 *   ).writer
 *
 *   HiveExternalTableWriter.writeToExternalFndTable(initialDataFrame, writer)
 * }}}
 *
 * @param env The environment of the external Hive table. Should either be "prd" or "stg"
 * @param schemaName The database of the external foundation table. This is usually either "prd_mdf_lzn" or "stg_mdf_lzn"
 * @param tableName The external foundation table name.
 * @param insertColumnNames A sequence of all columns on the external foundation table, in order, including the partition column(s).
 * @param partitionColumns [OPTIONAL] A sequence of only the partition column(s). Defaults to `Seq("report_d")`
 * @param retryCount [OPTIONAL] Sets how many attempts to retry in case there are failures encountered with the insert to Hive external table.
 *                            Defaulted to 3.
 * @param retrySleep [OPTIONAL] Sets how long to sleep in between retry attempts. Defaulted to 3 seconds.
 * @param mode [OPTIONAL] Used to specify the mode either saving to local filesystem or writing to external Hive table. Defaults to "overwrite"
 */
final case class HiveExternalAtomicTable(env: String, schemaName: String, tableName: String, insertColumnNames: Seq[String],
                                         partitionColumns: Seq[String] = MarketingConstants.DEFAULT_PARTITION_COLUMNS,
                                         retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                         retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                                         mode: String = CoreConstants.OVERWRITE,
                                         repartition: Boolean = true, recordFinalCount: Boolean = false
                                 ) extends HiveExternalTable {
  val writer: HiveExternalTableWriter = initWriter(env,insertColumnNames,partitionColumns,schemaName,tableName,
    retryCount = retryCount,retrySleep = retrySleep,mode = mode,repartition = repartition, recordFinalCount = recordFinalCount
  )
}

/**
 * Initialize a [[HiveExternalTableWriter]] class based on the parameters for an External Hive Foundation Table.
 * This case class should ONLY be used for local testing, NOT for stage or production definitions. The output of the
 * companion DataFrame will be written to the local filesystem rather than Hive. Use [[HiveExternalFoundationTable]] for
 * writing to Hive instead. The writer value will be passed to [[HiveExternalTableWriter.writeToExternalFndTable]] like this:
 * {{{
 *   //The finalDataFrame represents the very last DF,
 *   //after passing all DQ checks, transformations, cleansing, etc.
 *   val finalDataFrame: DataFrame
 *
 *   val writer: HiveExternalTableWriter = LocalFoundationTable(
 *     schemaName = "stg_mdf_fnd",
 *     tableName = "some_table_name",
 *     insertColumnNames = Seq("column1","column2",etc),
 *     externalLoc = "src/test/Data/hive/stg_mdf_fnd/some_table_name/"
 *   ).writer
 *
 *   HiveExternalTableWriter.writeToExternalFndTable(finalDataFrame, writer)
 * }}}
 *
 * @param schemaName The database of the external foundation table. In local testing, this is usually "stg_mdf_fnd"
 * @param tableName The external foundation table name.
 * @param insertColumnNames A sequence of all columns on the external foundation table, in order, including the partition column(s).
 * @param externalLoc Where to write the output file in the local filesystem to.
 * @param partitionColumns [OPTIONAL] A sequence of only the partition column(s). Defaults to `Seq("report_d)`
 * @param saveFormat [OPTIONAL] The file format to save the DataFrame to. Defaults to "orc"
 * @param retryCount [OPTIONAL] Sets how many attempts to retry in case there are failures encountered with the insert to Hive external table.
 *                            Defaulted to 3.
 * @param retrySleep [OPTIONAL] Sets how long to sleep in between retry attempts. Defaulted to 3 seconds.
 * @param mode [OPTIONAL] Used to specify the mode either saving to local filesystem or writing to external Hive table. Defaults to "overwrite"
 */
final case class LocalExternalFoundationTable(schemaName: String, tableName: String, insertColumnNames: Seq[String], externalLoc: String,
                                              partitionColumns: Seq[String] = MarketingConstants.DEFAULT_PARTITION_COLUMNS,
                                              saveFormat: String = CoreConstants.ORC,
                                              retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                              retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                                              mode: String = CoreConstants.OVERWRITE, recordFinalCount: Boolean = true) extends HiveExternalTable {
  val writer: HiveExternalTableWriter = initWriter(MarketingConstants.LOCAL_ENV,insertColumnNames,partitionColumns,schemaName,tableName,externalLoc,saveFormat,
    retryCount = retryCount, retrySleep = retrySleep, mode = mode, recordFinalCount = recordFinalCount)
}

/**
 * Initialize a [[HiveExternalTableWriter]] class based on the parameters for an External Hive Atomic Table.
 * This case class should ONLY be used for local testing, NOT for stage or production definitions. The output of the
 * companion DataFrame will be written to the local filesystem rather than Hive. Use [[HiveExternalAtomicTable]] for
 * writing to Hive instead. The writer value will be passed to [[HiveExternalTableWriter.writeToExternalFndTable]] like this:
 * {{{
 *   //The initialDataFrame represents the very first DF,
 *   //after reading from file and no further transformations are performed.
 *   val initialDataFrame: DataFrame
 *
 *   val writer: HiveExternalTableWriter = LocalAtomicTable(
 *     schemaName = "stg_mdf_lzn",
 *     tableName = "some_table_name",
 *     insertColumnNames = Seq("column1","column2",etc),
 *     externalLoc = "src/test/Data/hive/stg_mdf_lzn/some_table_name/"
 *   ).writer
 *
 *   HiveExternalTableWriter.writeToExternalFndTable(initialDataFrame, writer)
 * }}}
 *
 * @param schemaName The database of the external atomic table. In local testing, this is usually "stg_mdf_lzn"
 * @param tableName The external atomic table name.
 * @param insertColumnNames A sequence of all columns on the external atomic table, in order, including the partition column(s).
 * @param externalLoc Where to write the output file in the local filesystem to.
 * @param partitionColumns [OPTIONAL] A sequence of only the partition column(s). Defaults to `Seq("report_d)`
 * @param saveFormat [OPTIONAL] The file format to save the DataFrame to. Defaults to "csv"
 * @param retryCount [OPTIONAL] Sets how many attempts to retry in case there are failures encountered with the insert to Hive external table.
 *                            Defaulted to 3.
 * @param retrySleep [OPTIONAL] Sets how long to sleep in between retry attempts. Defaulted to 3 seconds.
 * @param mode [OPTIONAL] Used to specify the mode either saving to local filesystem or writing to external Hive table. Defaults to "overwrite"
 */
final case class LocalExternalAtomicTable(schemaName: String, tableName: String, insertColumnNames: Seq[String], externalLoc: String,
                                          partitionColumns: Seq[String] = MarketingConstants.DEFAULT_PARTITION_COLUMNS,
                                          saveFormat: String = CoreConstants.CSV,
                                          retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                          retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                                          mode: String = CoreConstants.OVERWRITE,
                                          recordFinalCount: Boolean = false) extends HiveExternalTable {
  val writer: HiveExternalTableWriter = initWriter(MarketingConstants.LOCAL_ENV,insertColumnNames,partitionColumns,schemaName,tableName,externalLoc,saveFormat,
    retryCount = retryCount, retrySleep = retrySleep, mode = mode, recordFinalCount = recordFinalCount)
}

final case class LocalExternalDirectory(env: String, sourceDb: String, sourceTable: String, selectColumnNames: Seq[String], writeToDir: String,
                                        saveFormat: String = CoreConstants.CSV,
                                        retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                        retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                                        recordFinalCount: Boolean = true) extends HiveExternalTable {
  val writer: HiveExternalTableWriter = initWriter(env,selectColumnNames,Seq(""),sourceDb,sourceTable,writeToDir,saveFormat,retryCount,retrySleep,
    dirIsLocal = true, recordFinalCount = recordFinalCount)
}

final case class HdfsExternalDirectory(env: String, sourceDb: String, sourceTable: String, selectColumnNames: Seq[String], writeToDir: String,
                                        saveFormat: String = CoreConstants.ORC,
                                        retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                        retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP, recordFinalCount: Boolean = true) extends HiveExternalTable {
  val writer: HiveExternalTableWriter = initWriter(env,selectColumnNames,Seq(""),sourceDb,sourceTable,writeToDir,saveFormat,retryCount,retrySleep,
    recordFinalCount = recordFinalCount)
}
