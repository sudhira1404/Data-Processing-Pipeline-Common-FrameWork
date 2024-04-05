package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.constants.ServiceConstants.MILLI_TO_NANO_FACTOR
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants.{INFLUX_METRICS_EXT_TABLENAME, INFLUX_METRICS_EXT_TABLE_INSERT_COLUMN_NAMES, INFLUX_METRICS_EXT_TABLE_PARTITION_COLUMN_NAMES}
import com.tgt.dse.mdf.common.pipeline.exceptions.HiveWriteException
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.readMaxPartitionAndCountFromTable
import com.tgt.dse.mdf.common.pipeline.util.DateTimeUtils.epochToDateTime
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.SparkSession

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

object PushCustomInfluxMetric extends LoggingTrait   {


  /**
   * Push custom partition metrics to influx with default influxCustomMeasurementName=mdf_table_partition_details mdf_table_partition_details
   * and then appended with underscore and env.
   * Metrics-(MaxPartitionTable,CountOfMaxPartitionTable,SlaTable,MaxPartitionDateLagInDaysTable,DataLoadDateTable,TableName)
   * @param pipeLineConfig:                 pipeLineConfig
   * Members
   * maxPartitionDateLagInDays:             Defaulted to 1.Overide in the application conf file("app.maxPartitionDateLagInDays")
   * partitionDateColName:                  Defaulted to "report_d".Partition column in the table.Overide in the application conf file("app.coreHistPartitionColumnName")
   * setPipeLineMetadataCustomInfluxMetric: Defaulted to true.If set ,will set  max partition date and it count and table load date in the
   *                                        pipelinemetadataservice as MaxPartitionTable,CountOfMaxPartitionTable,dataLoadDateTable which are pushed to influx.
   *                                        Overide in the application conf file(app.setPipeLineMetadataCustomInfluxMetric=false)
   * pushCustomInfluxMetric:                Defaulted to true.If set ,will set  max partition date and it count and table load date and sla and
   *                                        MaxPartitionDateLagInDaysTable value(field metrics) and TableName(tag metrics) in the
   *                                        influxCustomMeasurementName as MaxPartitionTable,CountOfMaxPartitionTable,SlaTable,
   *                                        MaxPartitionDateLagInDaysTable,DataLoadDateTable,TableName which are pushed to influx.
   *                                        Overide in the application conf file(app.pushCustomInfluxMetric=false)
   * influxCustomMeasurementName:           Defaulted to mdf_table_partition_details and then appended with underscore and env.
   *                                        This is the measurement name to push the custom influx metrics.Overide in the application conf file(app.influxCustomMeasurementName)
   *                                        (MaxPartitionTable,CountOfMaxPartitionTable,SlaTable,MaxPartitionDateLagInDaysTable,DataLoadDateTable,TableName)
   * readAlpaca:                            Defaulted to false.If set will read from alpaca
   *                                        else if table is external, will read using spark sql else if managed ,will read using 2 step
   *                                        Overide in the application conf file(app.readAlpaca=true)
   * @return    Boolean.
   */
  @SuppressWarnings(Array("All"))
  def pushCustomInfluxMetricWrapper(pipeLineConfig: PipeLineConfig)(implicit spark: SparkSession): Boolean = {

    var bool = true
    //if (pipeLineConfig.config.getString(MarketingConstants.APP_ENV) != "local") {
    var maxPartitionDateLagInDays =1
    var partitionDateColName ="report_d"
    var pushCustomInfluxMetric =true
    var setPipeLineMetadataCustomInfluxMetric =true
    var influxCustomMeasurementName= "mdf_table_partition_details"
    var readAlpaca= false

    try {
      maxPartitionDateLagInDays = pipeLineConfig.config.getInt(MarketingConstants.APP_MAXPARTITIONDATELAGINDAYS)
    }
    catch {
      case _: Exception => log.info("maxPartitionDateLagInDays will be defaulted to 1 for custom influx metric maxPartitionDateLagInDays." +
        "To Overide,add in application config file for app.maxPartitionDateLagInDays")
    }
    try {
      partitionDateColName = pipeLineConfig.config.getString(MarketingConstants.APP_coreHistPartitionColumnName)
    }
    catch {
      case _: Exception => log.info("coreHistPartitionColumnName will be defaulted to report_d for custom influx metric coreHistPartitionColumnName. " +
        "To Overide,add in application config file for 'app.coreHistPartitionColumnName")
    }
    try {
      influxCustomMeasurementName = pipeLineConfig.config.getString(MarketingConstants.APP_INFLUXCUSTOMMEASUREMENTNAME)
    }
    catch {
      case _: Exception => log.info("influxCustomMeasurementName will be defaulted to mdf_table_partition_details." +
        "To Overide,add in application config file for 'app.influxCustomMeasurementName")
    }
    try {
     // if (pipeLineConfig.config.getString(MarketingConstants.APP_ENV) != "local") {
        pushCustomInfluxMetric = pipeLineConfig.config.getBoolean(MarketingConstants.APP_PUSHCUSTOMINFLUXMETRIC)
      //} else {
        pushCustomInfluxMetric = false
      //}

    }
    catch {
      case _: Exception => log.info(s"pushCustomPartitionMetric flag will be defaulted to true and " +
        s"will push custom influx metric to influxCustomMeasurementName $influxCustomMeasurementName." +
        "To disable,make it false in application config file for 'app.pushCustomInfluxMetric")
    }
    try {
      setPipeLineMetadataCustomInfluxMetric = pipeLineConfig.config.getBoolean(MarketingConstants.APP_SETPIPELINEMETADATACUSTOMINFLUXMETRIC)
    }
    catch {
      case _: Exception => log.info("setPipeLineMetadataCustomInfluxMetric flag will be defaulted to true and " +
        s"will push custom influx metric to app.influxdb.measurementName." +
        "To disable,make it false in application config file for app.setPipeLineMetadataCustomInfluxMetric")
    }

    try {
      readAlpaca = pipeLineConfig.config.getBoolean(MarketingConstants.APP_READALPACA)
    }
    catch {
      case _: Exception => log.info("readAlpaca will be defaulted to false." +
        "This will read using alpaca read if set to true else will read from external using spark sql or managed using 2 step read " +
        "To Overide,add in application config file for app.readAlpaca")
    }

    val schemaName = pipeLineConfig.config.getString(MarketingConstants.APP_FNDSCHEMA)
    val tableName = pipeLineConfig.config.getString(MarketingConstants.APP_coreHistTable)
    val env = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    bool = {
      if (pushCustomInfluxMetric) {
        val influxMeasurementName = influxCustomMeasurementName + "_" + env
        readCustomInfluxMetricFromTableAndPushToInflux(
          pipeLineConfig,
          partitionDateColName,
          schemaName,
          tableName,
          influxMeasurementName,
          maxPartitionDateLagInDays,
          setPipeLineMetadataCustomInfluxMetric,
          readAlpaca)
      } else {
        if (setPipeLineMetadataCustomInfluxMetric) {
          readCustomInfluxMetricFromTableAndPushToPipeLineMetadataService(
            pipeLineConfig,
            partitionDateColName,
            schemaName,
            tableName,
            maxPartitionDateLagInDays,
            readAlpaca
          )
          true
        } else {
          true
        }
      }
    }
    //}
    bool
  }

  /**
   * Push custom partition metrics to influx and if setPipeLineMetadataPartitionMetric is set will set pipelinemetadataservice object
   * @param partitionDateColName                partitionDateColName
   * @param setPipeLineMetadataPartitionMetric  If set ,will set  max partition date,count,table load date,maxPartitionDateLagInDays in the
   *                                            pipelinemetadataservice as MaxPartitionTable,CountOfMaxPartitionTable,
   *                                            DataLoadDateTable and MaxPartitionDateLagInDaysTable which are pushed to influx
   * @param schemaName                          schemaName
   * @param tableName                           tableName
   * @param influxMeasurementName:              influxMeasurementName
   * @param readAlpaca                          If set will read from alpaca
   *                                            else if table is external, will read using spark sql else if managed ,will read using 2 step
   * @return    Boolean
   */

  def readCustomInfluxMetricFromTableAndPushToInflux(   pipeLineConfig: PipeLineConfig,
                                                        partitionDateColName: String,
                                                        schemaName: String,
                                                        tableName: String,
                                                        influxMeasurementName:String,
                                                        maxPartitionDateLagInDays:Int,
                                                        setPipeLineMetadataPartitionMetric:Boolean,
                                                        readAlpaca:Boolean
                                                    )(implicit sparkSession: SparkSession): Boolean ={

    val env: String = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    val jobStartEpochTime = if (env !="local") {
      PipeLineMetadataService.getLongMetricValue("startEpochTime")
    }
    else {
      LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
    }
    val jobEndEpochTime = System.currentTimeMillis()/1000
    val jobProcessingTimeInSeconds = jobEndEpochTime - jobStartEpochTime
    val jobStartDateTime = epochToDateTime(jobStartEpochTime).replaceAll(":", ";")
    val jobEndDateTime = epochToDateTime(jobEndEpochTime).replaceAll(":", ";")
    val maxPartitionDateLagInDaysTable = maxPartitionDateLagInDays
    val sla: String = pipeLineConfig.config.getString(MarketingConstants.APP_SLA).replaceAll(" ", "").replaceAll(":", "")
    val readMaxPartitionAndCountDF = if (env == "local") {
      ("2022-07-13", 12345)
    } else {
      readMaxPartitionAndCountFromTable(pipeLineConfig,partitionDateColName, schemaName, tableName,readAlpaca)
    }
    val maxPartitionTable =  readMaxPartitionAndCountDF._1
    val countOfMaxPartitionTable=readMaxPartitionAndCountDF._2

    val dataLoadDateTable = LocalDate.now.toString
    //    val fieldMetrics =
    //      s"""MaxPartitionTable="${maxPartitionTable}",CountOfMaxPartitionTable="${countOfMaxPartitionTable}",SlaTable="${sla}"
    //         |,MaxPartitionDateLagInDaysTable="${maxPartitionDateLagInDaysTable}",DataLoadDateTable="${dataLoadDateTable}"""".stripMargin.replaceAll("\n","")

    val fieldMetrics =
      s"""MaxPartitionTable="${maxPartitionTable}",CountOfMaxPartitionTable="${countOfMaxPartitionTable}",SlaTable="${sla}"
         |,MaxPartitionDateLagInDaysTable="${maxPartitionDateLagInDaysTable}"
         |,DataLoadDateTable="${dataLoadDateTable}",JobStartDateTime="${jobStartDateTime}",JobEndDateTime="${jobEndDateTime}",JobProcessingTimeInSeconds="${jobProcessingTimeInSeconds}""""
        .stripMargin.replaceAll("\n","")
    val tagMetrics: String =  s"TableName=$schemaName.${tableName}"

    val bool =  if (env !="local") {
      pushToInfluxEndpoint(pipeLineConfig, influxMeasurementName, tagMetrics: String, fieldMetrics: String)
    } else true

    //to insert influx custom metrics to hive table
    //    pushCustomMetricToExternalTable(
    //      pipeLineConfig,
    //      maxPartitionTable,
    //      countOfMaxPartitionTable,
    //      sla,
    //      maxPartitionDateLagInDaysTable,
    //      dataLoadDateTable,
    //      jobStartDateTime.replaceAll(";", ":"),
    //      jobEndDateTime.replaceAll(";", ":"),
    //      jobProcessingTimeInSeconds)

    bool
  }

  /**
   * Push custom partition metrics to PipeLineMetadataService.
   * @param pipeLineConfig                     pipeLineConfig
   * @param partitionDateColName               Partition column in the table
   * @param schemaName                         schemaName
   * @param tableName                          tableName
   * @param maxPartitionDateLagInDays          maxPartitionDateLagInDays
   * @param readAlpaca                         If set will read from alpaca
   *                                           else if table is external, will read using spark sql else if managed ,will read using 2 step
   * @return    tuple of (maxPartition, countOfMaxPartition)
   */

  def readCustomInfluxMetricFromTableAndPushToPipeLineMetadataService(
                                                                       pipeLineConfig: PipeLineConfig,
                                                                       partitionDateColName: String,
                                                                       schemaName: String,
                                                                       tableName: String,
                                                                       maxPartitionDateLagInDays:Int,
                                                                       readAlpaca:Boolean
                                                                     )(implicit sparkSession: SparkSession) ={


    val readMaxPartitionAndCountDF = readMaxPartitionAndCountFromTable(pipeLineConfig,partitionDateColName, schemaName, tableName,
      readAlpaca)

    val maxPartitionTable =  readMaxPartitionAndCountDF._1
    val countOfMaxPartitionTable=readMaxPartitionAndCountDF._2
    val dataLoadDateTable = LocalDate.now.toString
    val maxPartitionDateLagInDaysTable = maxPartitionDateLagInDays

    PipeLineMetadataService.setStringMetricValue("MaxPartitionTable", maxPartitionTable, influxField = true)
    PipeLineMetadataService.setLongMetricValue("CountOfMaxPartitionTable", countOfMaxPartitionTable, influxField = true)
    PipeLineMetadataService.setStringMetricValue("DataLoadDateTable", dataLoadDateTable, influxField = true)
    PipeLineMetadataService.setLongMetricValue("MaxPartitionDateLagInDaysTable", maxPartitionDateLagInDaysTable.toLong, influxField = true)


    (maxPartitionTable, countOfMaxPartitionTable)
  }


  /**
   * Push custom influx metrics to influx endpoint
   * @param pipeLineConfig:                     pipeLineConfig
   * @param influxMeasurementName:              influxMeasurementName
   * @param tagMetrics:                         tagMetrics
   * @param fieldMetrics:                       fieldMetrics
   * @return   Boolean
   */

  def pushToInfluxEndpoint(
                               pipeLineConfig: PipeLineConfig,
                               influxMeasurementName: String,
                               tagMetrics: String,
                               fieldMetrics: String
                             ): Boolean = {

    val blossomId =  pipeLineConfig.config.getString(MarketingConstants.APP_BLOSSOMID)
    val grafanaUrl = pipeLineConfig.config.getString(MarketingConstants.APP_INFLUXDB_GRAFANAENDPOINT)
    val EMPTYSTRING = ""
    val COMMA = ","
    val SPACE = " "
    val EQUAL = "="
    val COLON = ":"

    log.info("tagMetrics for posting custom influx metrics=>" + tagMetrics)
    log.info("fieldMetrics for posting custom influx metrics=>" + fieldMetrics)

    val influxMessage: String = (tagMetrics
      .replaceAll("[\"{\\}|]", EMPTYSTRING)
      .stripPrefix(COMMA)
      .stripSuffix(COMMA) + SPACE +
      fieldMetrics
        //    .replaceAll("[\"{\\}|]", EMPTYSTRING)
        .replaceAll("[{\\}|]", EMPTYSTRING)
        .stripPrefix(COMMA)
        .stripSuffix(COMMA))
      .stripPrefix(COMMA)
      .stripSuffix(COMMA)
      .replaceAll("\\|", "\"")

    val PostInfluxMessage: String = influxMessage
      .replaceAll(COLON, EQUAL)
      .replaceAll("[{\\}]", EMPTYSTRING)
      .replaceAll("[{\\}]", EMPTYSTRING)
    if (PostInfluxMessage.contains("=,") || PostInfluxMessage.contains("= ")) {
      throw new Exception(
        "Influx Message format incorrect as the metric value is blank for few keys for custom influx metrics"
      )
    }

    val grafanaPostTs: String =  {System.currentTimeMillis() * MILLI_TO_NANO_FACTOR}
      .toString
    val influxDbFormat: String =
      influxMeasurementName + ",_blossom_id=" + blossomId + COMMA + PostInfluxMessage + SPACE + grafanaPostTs
    log.info("InfluxMessage that will be pushed for custom influx metrics " + influxDbFormat)
    val post = new HttpPost(grafanaUrl)
    post.setEntity(new StringEntity(influxDbFormat))
//    import org.apache.http.conn.ssl.SSLConnectionSocketFactory
//    val builder = new SSLContextBuilder()
//    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy())
//    val sslConnectionFactory = new SSLConnectionSocketFactory(builder.build,NoopHostnameVerifier.INSTANCE)
    val response = HttpClientBuilder.create
                                    //.setSSLSocketFactory(sslConnectionFactory)
                                    .build
                                    .execute(post)
    log.info(" InfluxDB post response for custom influx metrics : " + response)

    if (response.toString.contains("204")) {
      log.info("Influx push successful for custom influx metrics")
      true
    } else {
      log.info("Influx push failed for custom influx metrics")
      false
    }
  }

  /**
   * Push custom partition metrics to external table(custom_influx_metrics)
   * @param pipeLineConfig                     pipeLineConfig
   * @param maxPartition                       maxPartition
   * @param countOfMaxPartition                countOfMaxPartition
   * @param maxPartitionDateLagInDays          maxPartitionDateLagInDays
   * @param dataLoadDate                       dataLoadDate
   * @param jobStartDateTime                   jobStartDateTime
   * @param jobEndDateTime                     jobEndDateTime
   * @param jobProcessingTimeInSeconds         jobProcessingTimeInSeconds

   * @return   Unit
   */

  def pushCustomMetricToExternalTable(
                                  pipeLineConfig: PipeLineConfig,
                                  maxPartition: String,
                                  countOfMaxPartition:Long,
                                  sla:String,
                                  maxPartitionDateLagInDays:Int,
                                  dataLoadDate:String,
                                  jobStartDateTime: String,
                                  jobEndDateTime: String,
                                  jobProcessingTimeInSeconds: Long
  )(implicit sparkSession: SparkSession): Unit ={

    val env = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    if (env !="local")  {
      import sparkSession.implicits._
      logInfo("Writing data to external Hive table.")
      //val fndExtLocation: String = MarketingCommonUtils.appendSlashInEnd(pipeLineConfig.config.getString(MarketingConstants.APP_FNDEXTLOCATION))
      val tableName = INFLUX_METRICS_EXT_TABLENAME
      val schema = pipeLineConfig.config.getString(MarketingConstants.APP_FNDSCHEMA)
      val data = Seq(s"$schema.$tableName,$dataLoadDate,$maxPartition,$countOfMaxPartition,$sla,$maxPartitionDateLagInDays,$jobStartDateTime,$jobEndDateTime,$jobProcessingTimeInSeconds")
      val inputDF = data.toDF()
      val successfulWrite = HiveExternalTableWriter.writeToExternalFndTable(inputDF, HiveExternalTableWriter(
        env,
        insertColumnNames = INFLUX_METRICS_EXT_TABLE_INSERT_COLUMN_NAMES,
        partitionColumnNames = INFLUX_METRICS_EXT_TABLE_PARTITION_COLUMN_NAMES,
        dirWriteLocation = "",
        schema = schema,
        tableName = tableName))(sparkSession)
      if (successfulWrite) {
        log.info("Successfully wrote to external Hive table")
      } else {
        throw HiveWriteException("Failed while writing to Hive table")
      }
    }
  }

}
