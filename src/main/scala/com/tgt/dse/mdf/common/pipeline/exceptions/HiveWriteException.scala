package com.tgt.dse.mdf.common.pipeline.exceptions

import com.tgt.dsc.kelsa.datapipeline.core.exceptions.DataPipeLineException

final case class HiveWriteException(
                               message: String,
                               cause: Throwable = None.orNull
                             )
  extends DataPipeLineException(message, cause)
