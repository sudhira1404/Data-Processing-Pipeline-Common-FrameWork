package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.constants.{DPPMetricsConstants, TimelinessConstants}
import com.tgt.dsc.kelsa.datapipeline.core.dataportal.DataCertificationService
import com.tgt.dsc.kelsa.datapipeline.core.service.{PipeLineMetadataService, SSLCredentialProvider}
import com.tgt.dsc.kelsa.datapipeline.core.types.DataPortalTimeliness
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils

import java.time.Instant
import java.util.Properties
import scala.util.{Failure, Success}

object TimelinessService extends LoggingTrait {

  /***
   * Initialize the DataPortalTimeliness by reading the data from previously set values from PipeLineMetadataService
   * @param workflowId The Oozie workflow ID
   * @return DataPortalTimeliness object initialized
   */
  def initializeTimelinessDetails(workflowId: String): DataPortalTimeliness ={

    //Based on your business logic and data find out freshness of data(dateOfData) and final records processed.
    //In the pipeline set the value in PipeLineMetadataService and that we can get it
    val rowCount : Long =  Option(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.INPUTROWSCOUNT))
      .getOrElse(0)
    val dateOfData : Long = Option(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DATEOFDATA))
      .getOrElse(Instant.now().toEpochMilli)

    DataPortalTimeliness(workflowId,
      dateOfData,
      rowCount,
      TimelinessConstants.OOZIE_BIG_RED_3,
      MarketingConstants.oozieURL)
  }


  /**
   * Push the timeliness to kafka by reading the standard settings from application.conf
   * @param appName Name of the application
   * @param workflowId The Oozie workflow ID
   * @param jceksFilePath The path to the jceks file containing username/password secrets for Kafka authentication
   * @param trustStoreFilePath The path to the truststore file.
   * @param keyStoreFilePath The path to the keystore file.
   * @return Status of message push
   */
  def pushTimeliness(appName: String, workflowId: String, jceksFilePath: String, trustStoreFilePath: String, keyStoreFilePath: String, env: String): Boolean = {

    if (!env.equals(MarketingConstants.LOCAL_ENV)) {
      //Read secret from hadoop credential file
      val secretsMap: Map[String, String] = getTimelinessKafkaSecretsMap(jceksFilePath)

      val kafkaTimelinessKafkaProperties: Properties = getTimelinessKafkaProducerProperties(secretsMap, appName, trustStoreFilePath, keyStoreFilePath)

      logInfo("Timeliness push started")
      val dataPortalTimeliness : DataPortalTimeliness = initializeTimelinessDetails(workflowId)

      //Put retry logic
      MarketingCommonUtils.withRetries(MarketingConstants.WRITE_RETRY_COUNT, MarketingConstants.WRITE_RETRY_SLEEP,
      "pushMessageToKafkaForDataPortalTimeliness")(DataCertificationService.pushMessageToKafkaForDataPortalTimeliness(
        dataPortalTimeliness,
        MarketingConstants.TIMELINESS_TOPIC_NAME,
        kafkaTimelinessKafkaProperties)
      ) match {
      case Success(value) =>
        logInfo(s"status of the message push is $value")
        true
      case Failure(exception) =>
        logError(s"Could not publish the timeliness, please reattempt by rerunning the flow or manually posting the message to kafka: ${exception.getMessage}")
        false
      }
    } else {
      log.info("Local testing, skipping publish Timeliness")
      true
    }
  }

  def getTimelinessKafkaProducerProperties(secretsMap: Map[String,String], appName: String, trustStoreLoc: String, keyStoreLoc: String): Properties = {
    val keystorePassword: String = secretsMap.getOrElse(MarketingConstants.SSL_KEYSTORE_PASSWORD,"")
    val truststorePassword: String = secretsMap.getOrElse(MarketingConstants.SSL_TRUSTSTORE_PASSWORD,"")

    val properties: Properties = new Properties()
    properties.put(MarketingConstants.SECURITY_PROTOCOL,MarketingConstants.TIMELINESS_SECURITY_PROTOCOL)
    properties.put(MarketingConstants.SSL_KEYSTORE_PASSWORD,keystorePassword)
    properties.put(MarketingConstants.SSL_TRUSTSTORE_LOCATION,trustStoreLoc)
    properties.put(MarketingConstants.BOOTSTRAP_SERVERS,MarketingConstants.TIMELINESS_BOOTSTRAP_SERVERS)
    properties.put(MarketingConstants.SSL_TRUSTSTORE_PASSWORD,truststorePassword)
    properties.put(MarketingConstants.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM,MarketingConstants.TIMELINESS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM)
    properties.put(MarketingConstants.VALUE_SERIALIZER,MarketingConstants.TIMELINESS_VALUE_SERIALIZER)
    properties.put(MarketingConstants.SSL_KEYSTORE_LOCATION,keyStoreLoc)
    properties.put(MarketingConstants.KEY_SERIALIZER,MarketingConstants.TIMELINESS_KEY_SERIALIZER)
    properties.put(MarketingConstants.CLIENT_ID,appName)
    properties.put(MarketingConstants.SSL_PROTOCOL,MarketingConstants.TIMELINESS_SSL_PROTOCOL)

    properties
  }

  def getTimelinessKafkaSecretsMap(jceKeyStore: String): Map[String, String] = {
    //Read secret from hadoop credential file
    val secretsMap: Map[String, String] = Map[String, String](
      MarketingConstants.SSL_KEYSTORE_PASSWORD -> SSLCredentialProvider
        .decryptPwdFromJceksFile(
          MarketingConstants.TIMELINESS_DP_KAFKA_KEY_STORE,
          jceKeyStore
        ).mkString(""),
      MarketingConstants.SSL_TRUSTSTORE_PASSWORD -> SSLCredentialProvider
        .decryptPwdFromJceksFile(
          MarketingConstants.TIMELINESS_DP_KAFKA_TRUST_STORE,
          jceKeyStore
        ).mkString("")
    )
    secretsMap
  }
}
