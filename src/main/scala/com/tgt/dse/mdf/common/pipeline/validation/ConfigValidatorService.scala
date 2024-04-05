package com.tgt.dse.mdf.common.pipeline.validation

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.exceptions.ConfigException
import com.tgt.dsc.kelsa.datapipeline.core.util.KelsaUtil.{validateDirPath, validateDirPathExists, validateEmptyValues, validateIntFormat}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import org.apache.spark.sql.SparkSession

/**
 * Confgiuration validator
 */
object ConfigValidatorService extends LoggingTrait {

  /**
   * Module to validate the correctness of the arguments passed by the user for the session
   *
   * @param pipeLineConfig runtime file params
   * @return valid : Boolean to check the correctness of the .conf file passed
   */

  def validateConfigProcessing(pipeLineConfig: PipeLineConfig,
                               optionalPaths: Seq[String] = Seq.empty[String],
                               mandatoryPaths: Seq[String] = Seq.empty[String])
                              (implicit spark: SparkSession): Boolean = {

    //If these paths does not exist then system will create them
    val optionalPathSeq: Seq[String] = {
      if (optionalPaths.isEmpty) {
        Seq(
          MarketingConstants.APP_CURRUPTRECORDLOCATION
        )
      } else {
        optionalPaths
      }
    }

    //These paths must exist
    val mandatoryPathSeq: Seq[String] = {
      if (mandatoryPaths.isEmpty) {
        Seq(
          MarketingConstants.APP_LOCKLOCATION,
          MarketingConstants.APP_PIPELINEFAILURES
        )
      } else {
        mandatoryPaths
      }
    }

    val validConfig = validateDirPath(pipeLineConfig.config, optionalPathSeq, state = true) &&
      validateDirPathExists(pipeLineConfig.config, mandatoryPathSeq, state = true)

    if (!validConfig) {
      val errorMessage: String = "Invalid configurations, please correct and rerun. hdfs folder may be missing for " +
        "APP_LOCKLOCATION, APP_PIPELINEFAILURES ..app exiting"
      logError(errorMessage)
      false
    } else {
      true
    }
  }

  /**
   * Validate paths which needs to be pre existing
   *
   * @param pipeLineConfig pipeline configurations
   * @return flag to indicate validation failure or success
   */
  def validateConfigurations(pipeLineConfig: PipeLineConfig, validateConfig: Seq[String] = Seq.empty[String]): Boolean = {
    //define the fields which are mandatory
    val configSeq: Seq[String] = if (validateConfig.isEmpty) MarketingConstants.CONFIG_SEQ else validateConfig
    val configSeqMandatory = Seq(
      "app.name",
      "app.blossomID",
      "app.teamName",
      "app.contactDetails")

    def contains[A](seq: Seq[A], item: A): Boolean = {
      seq.foldLeft(false)((r, c) => c.equals(item) || r)
    }

    configSeqMandatory.map(mandatoryString => if (!contains(configSeq, mandatoryString)) {
      log.info(s"need to provide $mandatoryString in the config file.Process will exit")
      throw ConfigException("Invalid configurations, please correct  and rerun ..app exiting")
    } else {
      true
    }
    )
    val configWithIntSeq: Seq[String] = Seq(
      MarketingConstants.COALESCEFACTOR
    )
    //Check if the fields defined are having non null values
    val validStringConfig: Boolean = validateEmptyValues(pipeLineConfig.config,
      configSeq,
      state = true)
    log.info(s"Configuration of String fields : $validStringConfig")
    //check if we are having correct int values
    val validIntConfig: Boolean = validateIntFormat(pipeLineConfig.config,
      configWithIntSeq,
      state = true)
    log.info(s"Configuration of Int fields : $validIntConfig")

    if (!validStringConfig || !validIntConfig) {
      throw ConfigException("Invalid configurations, please correct  and rerun ..app exiting")
    }

    validStringConfig && validIntConfig
  }

}