package com.tgt.dse.mdf.common.pipeline

import com.tgt.dsc.kelsa.datapipeline.core.config.{InfluxParam, PipeLineConfig}
import com.tgt.dsc.kelsa.datapipeline.core.constants.{CoreConstants, DPPMetricsConstants}
import com.tgt.dsc.kelsa.datapipeline.core.exceptions.{ConfigException, InvalidMetricsValueException, InvalidPathLockException, LockFileException}
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.util.LockServiceHelper.getLockValue
import com.tgt.dsc.kelsa.datapipeline.core.util.{FileServiceHelper, KelsaUtil, LockServiceHelper}
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants.{DATEFORMATTER, TIMELINESS_SSL_KEYSTORE_LOCATION, TIMELINESS_SSL_TRUSTSTORE_LOCATION}
import com.tgt.dse.mdf.common.pipeline.constants.{MarketingConstants, MarketingExceptions}
import com.tgt.dse.mdf.common.pipeline.exceptions._
import com.tgt.dse.mdf.common.pipeline.service.PushCustomInfluxMetric.pushCustomInfluxMetricWrapper
import com.tgt.dse.mdf.common.pipeline.service.{SlackPublish, TimelinessService}
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.loadConfigurationFile
import com.tgt.dse.mdf.common.pipeline.util.{DPPMetricUtils, MarketingCommonUtils}
import com.tgt.dse.mdf.common.pipeline.validation.ConfigValidatorService
import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneOffset}
import scala.util.{Failure, Try}

/**
 * Used to provide mappings to configs used by the main() method, as well as the start time of the pipeline run,
 * and the SparkSession.
 * @param pipeLineConfig The config object instantiated from the application.conf file
 */
final case class MarketingPipelineConfigs(pipeLineConfig: PipeLineConfig) {
  val localDateTime: LocalDateTime = LocalDateTime.now()
  val startEpochTime : Long = localDateTime.toEpochSecond(ZoneOffset.UTC)
  val JOB_START_TIME: String = localDateTime.format(DATEFORMATTER)

  val workFlowId : String = KelsaUtil.getWorkFLowID

  val appName: String = pipeLineConfig.config.getString(MarketingConstants.APP_NAME)
  val env: String = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
  val blossomID: String = pipeLineConfig.config.getString(MarketingConstants.APP_BLOSSOMID)
  val dqChannelId: String = pipeLineConfig.config.getString(MarketingConstants.APP_DQCHANNELID)
  val alertChannelId: String = pipeLineConfig.config.getString(MarketingConstants.APP_ALERTCHANNELID)
  val influxDbMeasurementName: String = pipeLineConfig.config.getString(MarketingConstants.APP_INFLUXDB_MEASUREMENTNAME)
  val influxDbEndpoint: String = pipeLineConfig.config.getString(MarketingConstants.APP_INFLUXDB_GRAFANAENDPOINT)
  val sla: String = pipeLineConfig.config.getString(MarketingConstants.APP_SLA)
  val lockPath: String = MarketingCommonUtils.appendSlashInEnd(pipeLineConfig.config.getString(MarketingConstants.APP_LOCKLOCATION))
  val jceksPath: String = if (pipeLineConfig.config.hasPath(MarketingConstants.APP_CONF_JCEKS_FILE_PATH)) {
    pipeLineConfig.config.getString(MarketingConstants.APP_CONF_JCEKS_FILE_PATH)
  } else {
    MarketingConstants.JCEKS_FILE_PATH
  }
  val truststorePath: String = if (pipeLineConfig.config.hasPath(MarketingConstants.APP_CONF_TRUSTSTORE_FILE_PATH)) {
    pipeLineConfig.config.getString(MarketingConstants.APP_CONF_TRUSTSTORE_FILE_PATH)
  } else {
    //MarketingConstants.SSL_TRUSTSTORE_LOCATION
    TIMELINESS_SSL_TRUSTSTORE_LOCATION
  }
  val keystorePath: String = if (pipeLineConfig.config.hasPath(MarketingConstants.APP_CONF_KEYSTORE_FILE_PATH)) {
    pipeLineConfig.config.getString(MarketingConstants.APP_CONF_KEYSTORE_FILE_PATH)
  } else {
    //MarketingConstants.SSL_KEYSTORE_LOCATION
    TIMELINESS_SSL_KEYSTORE_LOCATION
  }

  implicit val sparkSession: SparkSession = MarketingCommonUtils.createSparkSession(pipeLineConfig, env)

}

/**
 * Developers should extend this Trait on the object which has the main() method in it, eg. the object referred to
 * in the spark-submit call. This has all the startup and shutdown related common methods in it, such as handling lock
 * files, loading and validating configs, etc.
 */
trait MarketingPipelineMainTrait extends LoggingTrait {

  /**
   * This should be the very first thing called in a pipeline's main method. It will load the configs from the file
   * specified in the spark-submit arguments, as well as validate the configs are properly formatted. It will return
   * a MarketingPipelineConfigs object with the configs needed to run the pipeline, the start time of the pipeline, and
   * the SparkSession.
   * @param args The array of arguments passed in the spark-submit
   * @return An initialized MarketingPipelineConfigs object
   * @throws ConfigException If configs are invalid, force the app to exit right away
   */
  def loadPipeLineConfigurations(args: Array[String],pathsToValidate:Seq[String]=Seq.empty[String],validateConfig:Seq[String]=Seq.empty[String],
                                 mandatoryPaths: Seq[String] = Seq.empty[String]):MarketingPipelineConfigs = {
    //Create Application Config by reading application.conf configuration file to get paths like inputPath,templateLocation
    MarketingCommonUtils.usage(args)
    //Load configuration file and check required parameters are not null
    println("inside loadConfigurationFile")
    val pipeLineConfig: PipeLineConfig = loadConfigurationFile(args(0),validateConfig)
    //get lock Path, for avoiding retrigger of job
    val lockPath = MarketingCommonUtils.appendSlashInEnd(pipeLineConfig.config.getString(MarketingConstants.APP_LOCKLOCATION))
    logInfo(s"lockPath is : $lockPath")

    if (pipeLineConfig != null && lockPath != null) {
      val config: MarketingPipelineConfigs = MarketingPipelineConfigs(pipeLineConfig)
      //Build a path validation sequence from configs to use during config validation step
      //These paths need to be created if they don't already exist
      val pathsToValidateOrCreate =  if (pathsToValidate.isEmpty){
        Seq(
          MarketingConstants.APP_INPUTLOCATION,
          MarketingConstants.APP_FILEAPILANDINGLOCATION,
          MarketingConstants.APP_CURRUPTRECORDLOCATION,
          MarketingConstants.APP_ARCHIVEPARTITIONRECORDSLOCATION,
          MarketingConstants.APP_EXISTINGRECORDSLOCATION
        )
      }
      else {
        pathsToValidate
      }

      val mandatoryPath: Seq[String] =
        if (mandatoryPaths.isEmpty) {
          Seq(
            MarketingConstants.APP_LOCKLOCATION,
            MarketingConstants.APP_PIPELINEFAILURES
          )
        } else {
          mandatoryPaths
        }
      //validate if the configured locations are pre existing
      val validConfig: Boolean =
        ConfigValidatorService.validateConfigProcessing (pipeLineConfig, optionalPaths = pathsToValidateOrCreate,
          mandatoryPaths = mandatoryPath )(config.sparkSession)
      if (validConfig) {
        log.info (s"Configuration status : $validConfig")
        //Try to create the lock file
        createLock(config.lockPath,config.appName)(config.sparkSession)
        //Initialize the metrics cache
        DPPMetricUtils.initializeMetricCollector(pipeLineConfig)

        DPPMetricUtils.captureApplicationConfigMetrics("1.2.1","1.8.4",config.startEpochTime,config.workFlowId,config.sparkSession)

        SlackPublish.sendSlackMessage(s"${config.appName} DPP pipeline to load Core Hist table started", config.dqChannelId)
        log.info(s"Starting the ${config.appName} DPP process at ${config.JOB_START_TIME}")
        //Return the config
        config
      } else {
        throw ConfigException("One or more configs are invalid. Check logs for the configs with issues.")
      }
    } else {
      throw ConfigException("Exception in configurations.. app exiting")
    }
  }

  /**
   * Call this for every Exception caught in the main() method.
   * @param exception The throwable which was caught.
   * @param logMessage The name of the cause of the exception, to be used in log messages.
   * @param config The initialized MarketingPipelineConfigs object.
   */
  def exceptionError(exception: Exception,
                     logMessage: String,
                     config: MarketingPipelineConfigs): Unit = {
    val exceptionName: String = exception.getClass.getSimpleName
    SlackPublish.sendSlackMessage(
      s"${config.appName} DPP pipeline to load Core Hist table FAILED - $exceptionName-${exception.getMessage}",
      config.alertChannelId
    )
    DPPMetricUtils.errorMetricDetails(exception)
    log.error(s"Exception in $logMessage .. app exiting", exception)
  }

  /**
   * Call this upon a successful pipeline run.
   * @param config The initialized MarketingPipelineConfigs object.
   */
  def publishSuccess(config: MarketingPipelineConfigs): Unit = {
    //Generate date of data and store in metrics cache so it can be used by both Timeliness and Grafana
    DPPMetricUtils.setDateOfData()
    //mark the successful processing of data
    TimelinessService.pushTimeliness(config.appName, config.workFlowId, config.jceksPath, config.truststorePath, config.keystorePath, config.env)

    SlackPublish.sendSlackMessage(
      s"${config.appName} DPP pipeline to load Core Hist table completed successfully",
      config.dqChannelId)

    PipeLineMetadataService.setStringMetricValue(DPPMetricsConstants.DPPJOBSTATUS,MarketingConstants.SUCCESS, influxField = true)
    log.info("Successfully logged success to Metrics cache")
  }

  /**
   * Use this to delete the lock file instead of directly using Kelsa's LockServiceHelper.deleteLock() method. There is
   * a bug in that method that concatenates the word "lock" to the end of the lockPath, causing it to fail to find the
   * lock file, since it's looking in one directory that doesn't match the lockPath. This method uses just the lockPath
   * to search for the lock file.
   * @param lockPath The path to the directory storing lock files.
   * @param appName The name of the application, from the MarketingPipelineConfigs.appName.
   * @param sparkSession The implicit spark session, from the MarketingPipelineConfigs.sparkSession.
   */
  def deleteLock(lockPath: String, appName: String)(implicit sparkSession: SparkSession): Unit = {
    try {
      //There is a bug in LockServiceHelper.deleteLock (lockPath,appName)(sparkSession)
      log.info(s"Checking for lock file in the location $lockPath")
      val results: Try[Unit] = Try(
        FileServiceHelper
          .deleteFile(
            lockPath.concat(CoreConstants.BACKSLASH + getLockValue(lockPath, appName))
          )
      )
      results match {
        case Failure(exception) =>
          log.error(
            s"Error while deleting the present lock from the location $lockPath for the application $appName: ${exception.getMessage}"
          )
        case _ =>
          log.info(s"Successfully deleted present lock from the location $lockPath for the application $appName")
      }
    } catch {
      case e: InvalidPathLockException =>
        log.error(s"Lock file doesn't exist at path specified: $lockPath for application: $appName. Full exception: ",e)
    }
  }

  /**
   * Checks if the lock file exists. If it doesn't, creates the lock file, else throws LockFileException.
   * @param lockPath The path to the directory storing lock files.
   * @param appName The name of the application, from the MarketingPipelineConfigs.appName.
   * @param sparkSession The implicit spark session, from the MarketingPipelineConfigs.sparkSession.
   * @return The name of the lock file.
   * @throws LockFileException When the lock file already exists.
   */
  def createLock(lockPath: String, appName: String)(implicit sparkSession: SparkSession): String = {
    val lockExists: Boolean = LockServiceHelper.isLockPresent(lockPath,appName)
    if (lockExists) {
      throw LockFileException(s"Lock already exists! Please check the lockPath: $lockPath for the appName: $appName to " +
        "verify it can be safely removed. If another instance of this DPP is already running, you must wait until it " +
        "completes before running this again.")
    } else {
      LockServiceHelper.createLock(lockPath, appName)
    }
  }

  /**
   * Push custom partition metrics to influx with default influxCustomMeasurementName=mdf_table_partition_details mdf_table_partition_details
   * and then appended with underscore and env.
   * Metrics-(MaxPartitionTable,CountOfMaxPartitionTable,SlaTable,MaxPartitionDateLagInDaysTable,DataLoadDateTable,TableName)
   * @return    Boolean
   */
  def pushCustomInfluxMetric(pipeLineConfig: PipeLineConfig)(implicit spark: SparkSession):Boolean={

    var resultTrueBool=true

    try {
      resultTrueBool= pushCustomInfluxMetricWrapper(pipeLineConfig)
      log.info(s"Return value after pushing custom influx metrics=>$resultTrueBool")
      if (resultTrueBool) {
        log.info("Successfully pushed custom influx metrics")
      }
      else {
        log.info("Failed to push custom influx metrics")
        resultTrueBool=false
      }
    } catch {
      case e : PushCustomInfluxMetricException  => log.error("Exception in pushCustomInfluxMetric: ", e);resultTrueBool=false
    }
    resultTrueBool
  }

  /**
   * Call this in the finally block of the main() method. Used to finish collecting metrics, and posting to Influx for
   * Grafana reporting, clearing the Spark cache, and stopping the Spark Session.
   * @param config The initialized MarketingPipelineConfigs object.
   * @param test True if this is a test run to prevent pushing metadata to Influx
   * @param raiseExitCode True if there was an exception handled earlier that should raise the exit code of the app for
   *                      downstream processes to see the pipeline run was a failure.
   */
  def finalizePipelineRun(config: MarketingPipelineConfigs, test: Boolean = false, raiseExitCode: Boolean = false): Unit = {
    val finalOutputRows: Long = {
      try {
        PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.INPUTROWSCOUNT)
      } catch {
        case e: InvalidMetricsValueException => {
          log.error("Exception in trying to retrieve final output rows, so setting to 0. Full exception is: ", e)
          0
        }
      }
    }

    log.info(s"Final Output Rows to publish to Grafana: $finalOutputRows")
    PipeLineMetadataService.metadataFinalDPP(finalOutputRows)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.JOB_SLA, config.sla,influxField = true)

    val loadCompletionTime = LocalDateTime.now.format(MarketingConstants.DATEFORMATTER)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.LOADCOMPLETIONTIME, loadCompletionTime,influxField = true)
    log.info(s"Load completion time: $loadCompletionTime")
    val pushCustomInfluxMetricResult = pushCustomInfluxMetric(config.pipeLineConfig)(config.sparkSession)
    //metrics captured till this stage will be consolidated and can be pushed to influx or kafka or it can be logged

    val metricMessage: String =
      if (!test) {
        PipeLineMetadataService.metadataPost(
          config.pipeLineConfig,
          metricsKafkaPush = false,
          metricsInfluxPush = true,
          influxParameters = InfluxParam(
            config.blossomID,
            config.influxDbMeasurementName,
            config.influxDbEndpoint
          )
        )
      } else {"Testing, skip metric publish"}
    log.info (metricMessage)
    if (!test) {
      log.info ("clearing the cache...")
      config.sparkSession.catalog.clearCache ()
    }

    log.info ("delete the lock file...")
    deleteLock(config.lockPath, config.appName)(config.sparkSession)

    if (!test) {
      log.info("spArk session stop")
      config.sparkSession.stop ()
    }
    log.info ("PipeLine execution complete")
    if ((raiseExitCode || !pushCustomInfluxMetricResult) && !test) {
      log.error("Failures detected, will exit with raised error code")
      System.exit(1)
    }
  }


  @SuppressWarnings(Array("CyclomaticComplexity"))
  def exceptionHandler(exception:Throwable, config:MarketingPipelineConfigs): Unit = {
    exception match {
      case e: LockFileException => exceptionError(e, MarketingExceptions.LOCK_FILE.getError, config)
      case e: DataQualityException => exceptionError(e, MarketingExceptions.DATA_QUALITY.getError, config)
      case e: DateFormatException => exceptionError(e, MarketingExceptions.DATE_FORMAT.getError, config)
      case e: FileMoveException => exceptionError(e, MarketingExceptions.FILE_MOVE.getError, config)
      case e: FileCopyException => exceptionError(e, MarketingExceptions.FILE_COPY.getError, config)
      case e: SchemaException => exceptionError(e, MarketingExceptions.SCHEMA.getError, config)
      case e: ContractValidationException => exceptionError(e, MarketingExceptions.CONTRACT_VALIDATION.getError, config)
      case e: InvalidPipeLineOptionException => exceptionError(e, MarketingExceptions.INVALID_PIPELINE_OPTION.getError, config)
      case e: HiveWriteException => exceptionError(e, MarketingExceptions.HIVE_WRITE.getError, config)
      case e: DateSequenceException => exceptionError(e, MarketingExceptions.DATE_SEQUENCE.getError, config)
      case e: Exception => exceptionError(e, MarketingExceptions.GENERAL.getError, config)
    }
  }
}
