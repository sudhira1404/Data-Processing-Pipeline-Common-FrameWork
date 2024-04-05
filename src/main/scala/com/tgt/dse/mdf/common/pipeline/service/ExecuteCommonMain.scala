package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.{LoggingTrait, MarketingPipelineConfigs, MarketingPipelineMainTrait}

import scala.util.{Failure, Success, Try}


object ExecuteCommonMain extends LoggingTrait with MarketingPipelineMainTrait  {


  /**
   * Main entrypoint for the Main method to invoke.
   * @param args The command line arguments passed to the Spark session.
   * @param f A function that will produce a @MarketinPipelineConfigs object.
   * @param pathsToValidate A sequence of directory paths to validate exist. They will be created if missing.
   * @param validateConfig A sequence of config keys to check that are set.
   * @param mandatoryPaths A sequence of directory paths that must exist prior to the run. They will not be created if missing.
   * @param abortFlow True if the flow should raise an exit code when an Exception is thrown.
   *                  False to exit without a raised exit code when an Exception is thrown. Defaults to true.
   */
  def executeCommonMainPipeline(args: Array[String],
                                f: MarketingPipelineConfigs => Unit,
                                pathsToValidate:Seq[String]=Seq.empty[String],
                                validateConfig:Seq[String]=Seq.empty[String],
                                mandatoryPaths: Seq[String] = Seq.empty[String],
                                abortFlow:Boolean=true): Unit = {


    val config: MarketingPipelineConfigs = configHandler(args,pathsToValidate,validateConfig,mandatoryPaths)

    val failure = pipelineHandler(f,config)

    val test = (config.env == "local")
    val raiseExitCode = (abortFlow && !test && failure)
    finalizePipelineRun(config, test = test, raiseExitCode = raiseExitCode)

  }

  private def configHandler(args: Array[String],
                    pathsToValidateOrCreate:Seq[String],
                    validateConfig:Seq[String],
                    mandatoryConfig:Seq[String]): MarketingPipelineConfigs = {

    Try(loadPipeLineConfigurations(args,pathsToValidateOrCreate,validateConfig,mandatoryConfig))

    match {
      case Success(config: MarketingPipelineConfigs) => config
      case Failure(exception) => throw exception //throwing an exception to halt the run rather than invoke the exceptionHandler
    }
  }


  private def pipelineHandler(f: MarketingPipelineConfigs => Unit,config: MarketingPipelineConfigs): Boolean = {
    var failure = false
    Try(f(config))
    match {
      case Success(_) => publishSuccess(config)
      case Failure(a) => {
        failure = true
        exceptionHandler(a, config)
      }
    }
    failure

  }

}

