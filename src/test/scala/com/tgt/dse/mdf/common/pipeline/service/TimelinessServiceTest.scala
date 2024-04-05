package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.constants.{CoreConstants, DPPMetricsConstants, TimelinessConstants}
import com.tgt.dsc.kelsa.datapipeline.core.retry.RetryHandler
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.types.DataPortalTimeliness
import com.tgt.dsc.kelsa.datapipeline.core.util.KelsaUtil
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.util.DPPMetricUtils
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.util.Properties

class TimelinessServiceTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

  "TimelinessServiceTest object" should "do the following"

  it should "initialize the DataPortalTimeliness object properly" in {
    val count: Long = 12345L
    PipeLineMetadataService.setLongMetricValue(DPPMetricsConstants.INPUTROWSCOUNT,count)
    System.setProperty(CoreConstants.WF_ID,"wf_123")
    val workflowId = KelsaUtil.getWorkFLowID

    DPPMetricUtils.setDateOfData()
    val dataPortalTimeliness: DataPortalTimeliness = TimelinessService.initializeTimelinessDetails(workflowId)
    assertResult(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DATEOFDATA))(dataPortalTimeliness.dateOfData)
    assertResult(TimelinessConstants.OOZIE_BIG_RED_3)(dataPortalTimeliness.platform)
    assertResult(MarketingConstants.oozieURL)(dataPortalTimeliness.oozieURL)
    assertResult(workflowId)(dataPortalTimeliness.workflowID)
    assertResult(count)(dataPortalTimeliness.rowCount)
  }

  it should "return a Properties object initialized properly" in {
    val appName = pipeLineConfig.config.getString(MarketingConstants.APP_NAME)
    val keystorePassword = "foo"
    val truststorePassword = "bar"
    val secretsMap = Map(MarketingConstants.SSL_KEYSTORE_PASSWORD -> keystorePassword,
      MarketingConstants.SSL_TRUSTSTORE_PASSWORD -> truststorePassword)
    val pwd = System.getProperty("user.dir")
    val truststoreLoc: String = s"${pwd}/src/test/resources/some_file.jks"
    val keystoreLoc: String = s"${pwd}/src/test/resources/some_keystore_file.jks"

    val properties: Properties = TimelinessService.getTimelinessKafkaProducerProperties(secretsMap,appName,truststoreLoc, keystoreLoc)

    assertResult(MarketingConstants.TIMELINESS_SECURITY_PROTOCOL)(properties.getProperty(MarketingConstants.SECURITY_PROTOCOL))
    assertResult(keystorePassword)(properties.getProperty(MarketingConstants.SSL_KEYSTORE_PASSWORD))
    assertResult(truststoreLoc)(properties.getProperty(MarketingConstants.SSL_TRUSTSTORE_LOCATION))
    assertResult(MarketingConstants.TIMELINESS_BOOTSTRAP_SERVERS)(properties.getProperty(MarketingConstants.BOOTSTRAP_SERVERS))
    assertResult(truststorePassword)(properties.getProperty(MarketingConstants.SSL_TRUSTSTORE_PASSWORD))
    assertResult(MarketingConstants.TIMELINESS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM)(properties.getProperty(MarketingConstants.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM))
    assertResult(MarketingConstants.TIMELINESS_VALUE_SERIALIZER)(properties.getProperty(MarketingConstants.VALUE_SERIALIZER))
    assertResult(keystoreLoc)(properties.getProperty(MarketingConstants.SSL_KEYSTORE_LOCATION))
    assertResult(MarketingConstants.TIMELINESS_KEY_SERIALIZER)(properties.getProperty(MarketingConstants.KEY_SERIALIZER))
    assertResult(appName)(properties.getProperty(MarketingConstants.CLIENT_ID))
    assertResult(MarketingConstants.TIMELINESS_SSL_PROTOCOL)(properties.getProperty(MarketingConstants.SSL_PROTOCOL))

  }

  it should "initialize a map of secrets from jceks file" in {
    val pwd = System.getProperty("user.dir")
    val jceKeyStore: String = s"jceks://file/${pwd}/src/test/resources/test.jceks"

    val secretMap = TimelinessService.getTimelinessKafkaSecretsMap(jceKeyStore)
    assertResult("foo")(secretMap.get(MarketingConstants.SSL_KEYSTORE_PASSWORD).mkString(""))
    assertResult("bar")(secretMap.get(MarketingConstants.SSL_TRUSTSTORE_PASSWORD).mkString(""))
  }
}
