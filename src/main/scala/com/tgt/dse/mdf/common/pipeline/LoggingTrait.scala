package com.tgt.dse.mdf.common.pipeline

import org.slf4j.{Logger, LoggerFactory}

/**
 * This is extended on every object and case class that uses logging. Developers using this library should also extend
 * this trait to all custom objects and case classes which need to log. Supports writing log statements in different
 * styles, eg. "log.info()" or "logInfo" or "logger.info".
 */
trait LoggingTrait {

  val log: Logger = LoggerFactory.getLogger((this.getClass))

  //Maps "logger" to "log" so a developer can use "logger.info()" or "log.info()" interchangeably
  val logger: Logger = log

  /**
   * Maps "logInfo()" style to "log.info()" style
   * @param msg The message to log.
   */
  def logInfo(msg: String): Unit = log.info(msg)

  /**
   * Maps "logError()" style to "log.error()" style
   * @param msg The message to log.
   */
  def logError(msg: String): Unit = log.error(msg)

  /**
   * Maps "logWarn()" style to "log.warn()" style
   * @param msg The message to log.
   */
  def logWarn(msg: String): Unit = log.warn(msg)

  /**
   * Maps "logDebug()" style to "log.debug()" style
   * @param msg The message to log.
   */
  def logDebug(msg: String): Unit = log.debug(msg)

}