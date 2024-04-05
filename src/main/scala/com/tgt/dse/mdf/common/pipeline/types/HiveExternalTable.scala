package com.tgt.dse.mdf.common.pipeline.types

import com.tgt.dsc.kelsa.datapipeline.core.constants.CoreConstants
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.service.HiveExternalTableWriter

trait HiveExternalTable {

  def initWriter(env: String, insertColumnNames: Seq[String], partitionColumns: Seq[String],
                 schemaName: String, tableName: String, externalLoc: String = "", saveFormat: String = "",
                 retryCount: Int = MarketingConstants.WRITE_RETRY_COUNT, retrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                 mode: String = CoreConstants.OVERWRITE, dirIsLocal: Boolean = false,repartition: Boolean = true,
                 recordFinalCount: Boolean = true
                ): HiveExternalTableWriter = {
    HiveExternalTableWriter(
      env,
      insertColumnNames = insertColumnNames,
      partitionColumnNames = partitionColumns,
      dirWriteLocation = externalLoc,
      schema = schemaName,
      tableName = tableName,
      writeRetryCount = retryCount,
      writeRetrySleep = retrySleep,
      localSaveFormat = saveFormat,
      writeMode = mode,
      dirIsLocal = dirIsLocal,
      repartition=repartition,
      recordFinalCount=recordFinalCount
    )
    }

}
