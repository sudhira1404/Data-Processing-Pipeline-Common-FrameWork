package com.tgt.dse.mdf.common.pipeline.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jcraft.jsch.ChannelSftp
import com.springml.sftp.client.SFTPClient
import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.service.DataReadWriteHelper.readFromHiveManagedTableWith2StepProcess
import com.tgt.dsc.kelsa.datapipeline.core.types.HiveTableReadConfigDetails
import com.tgt.dsc.kelsa.datapipeline.core.util.KelsaUtil
import com.tgt.dse.mdf.common.pipeline.{LoggingTrait, MarketingPipelineConfigs}
import com.tgt.dse.mdf.common.pipeline.cleanse.MessageCleanser
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{ArrayIndexOutOfBoundsException, GeneralException, TransferFromHdfsToSftpException, TransferFromSftpToHdfsException, ZeroRecordInDataframeException}
import com.tgt.dse.mdf.common.pipeline.service.EmailService.sendEmailWrapper
import com.tgt.dse.mdf.common.pipeline.service.ExecuteCommonMain.deleteLock
import com.tgt.dse.mdf.common.pipeline.service.{EmailService, FileWriterService, SftpTransferService}
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService.deleteHiddenFile
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.{calculateFileSplit, getDistinctInputDates, handleEmptyTopic, readExternalInputTableToDF}
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.getFileNameAndExtensionFromFileName
import com.tgt.dse.mdf.common.pipeline.util.HdfsUtils.{hdfsFileRename, hdfsSaveFromDF}
import com.tgt.dse.mdf.common.pipeline.util.SftpUtils.disConnectSftpConnection
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects.{CommonPipeLineHelper, JoinCal}
import com.typesafe.config.ConfigException
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CommonExecutePipelineUtils extends LoggingTrait {

  /**
   * Convert map like string to map of strings.
   * If not able to convert will throw exception
   * like  "header->true,quote->\",escape->\",MultiLine->false,ignoreTrailingWhiteSpace-> true" and converts to map of string
   * and converts to map of strings.If string has comma in the value then it needs be double escaped like
   * "header->true,sep->\\,,mapreduce.fileoutputcommitter.marksuccessfuljobs->false"
   * @param str string
   * @return sequence of string
   */

  def convertStringToMap(str: String): Map[String, String] = {

    val map = try {
      //str.trim.split(",").map(_.split("->")).map(arr => arr(0).trim -> arr(1).trim).toMap
      str.trim.replaceAll("\\\\,","comma").trim.split(",").map(_.replaceAll("comma",",").split("->")).map(arr => arr(0).trim->arr(1).trim).toMap

    } catch {
      //abort
      case e: Exception => throw GeneralException(s"Exception in convertStringToMap.Check the config entry: $e")
    }

    //log.info("String to map value is " + map)
    map
  }

  /**
   * Convert comma separated string value to sequence collection.
   * If not able to convert will throw exception
   * like  "cols1,col2"and converts to sequence of strings
   *
   * @param str string
   * @return sequence of string
   */

  def convertStringToSeq(str: String): Seq[String] = {
    //replaceAll("\\s+","")
    //replaceAll(" +", "").trim

    val seq = try {
      str.trim.split(",").map(_.trim).toSeq
    } catch {
      //abort
      case e: Exception => throw GeneralException(s"Exception in convertStringToSeq.Check the config entry: $e")
    }

    //log.info("String to Seq value is " + seq)
    seq

  }

  /**
   * Convert comma separated string value to array collection.
   * If not able to convert will throw exception
   * like  "cols1,col2"and converts to array of strings
   *
   * @param str string
   * @return array of string
   */

  def convertStringToArray(str: String): Array[String] = {

    val arr = try {
      str.trim.split(",").map(_.trim).toArray
    } catch {
      //abort
      case e: Exception => throw GeneralException(s"Exception in convertStringToSeq.Check the config entry: $e")
    }

    //log.info("String to Array value is " + seq)
    arr

  }

  /**
   * Convert Json like string value to Seq[(String, String)].
   * If not able to convert will throw exception
   * like \"{\"Revenue\":\"revenue_a\", \"Salecost\":\"decimal(38,2)\"}\" ") and converts to sequence of string tuple
   *
   * @param str Json string
   * @return Sequence of string tuple
   */

  def convertJsonStringToListOfStringTuple(str: String): Seq[(String, String)] = {

    val stringInputWithoutNewline = str.replace("\n", "").trim.replaceAll(" +", " ").replaceAll("\\\\\"", "\"")

    val map = try {
      val mapper = new ObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.readValue(stringInputWithoutNewline, classOf[Map[String, String]])
    } catch {
      //abort
      case e: Exception => throw GeneralException(s"Exception in convertJsonStringToListOfStringTuple.Check the config entry: $e")
    }

    //log.info("Json to map value is " + map)
    map.toSeq

  }

  /**
   * Read comma separated string value from PipeLineConfig object and converts to Sequence collection.
   * If not defined in conf file then will default to Seq.empty[String]
   * like  "cols1,col2" and converts to Sequence
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return converted pipeLineConfigValue
   */

  def getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig: PipeLineConfig, applicationConstant: String): Seq[String]
  = {
    val pipeLineConfigValue: Seq[String] = try {
      val pipeLineConfigValue = pipeLineConfig.config.getString(applicationConstant)
      val pipeLineConfigValueConvert = convertStringToSeq(pipeLineConfigValue)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue + " which is converted to Seq[String] as :" + pipeLineConfigValueConvert)

      pipeLineConfigValueConvert
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to empty string and then converted to Seq.empty[String].To Overide,add in application config file for $applicationConstant as comma separated attributes")

        Seq.empty[String]
    }
    pipeLineConfigValue

  }

  /**
   * Read Map like string value from PipeLineConfig object and converts to Map[String, String].
   * If not defined in conf file then will default to Map[String, String]()
   * like  "header->true,quote->\",escape->\",MultiLine->false,ignoreTrailingWhiteSpace-> true" and converts to map of string
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return converted pipeLineConfigValue
   */

  def getValueFromPipeLineConfigAndConvertToMap(pipeLineConfig: PipeLineConfig, applicationConstant: String): Map[String, String]
  = {
    val pipeLineConfigValue: Map[String, String] = try {
      val pipeLineConfigValue = pipeLineConfig.config.getString(applicationConstant)
      val pipeLineConfigValueConvert = convertStringToMap(pipeLineConfigValue)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue + "  which is converted to to Map[String, String] as :" + pipeLineConfigValueConvert)

      pipeLineConfigValueConvert
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to empty string and then converted to Map[String, String]().To Overide,add in application config file for $applicationConstant as comma separated attributes")

        Map[String, String]()
    }
    pipeLineConfigValue

  }

  /**
   * Read Map like string value from PipeLineConfig object and converts to Map[String, String] and gets list of keys and list values as tuple
   * If not defined in conf file then will default to tuple of list of empty strings
   * like  "header->true,quote->\",escape->\",MultiLine->false,ignoreTrailingWhiteSpace-> true" and converts to map of string
   * and gets keys as list and values as list
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return converted pipeLineConfigValue
   */

  def getValueFromPipeLineConfigAndConvertToMapAndGetKeysAndValues(pipeLineConfig: PipeLineConfig, applicationConstant: String): (List[String], List[String])
  = {
    val pipeLineConfigValue = try {
      val pipeLineConfigValue = pipeLineConfig.config.getString(applicationConstant)
      val pipeLineConfigValueConvert = convertStringToMap(pipeLineConfigValue)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue + "  which is converted to to Map[String, String] as :" + pipeLineConfigValueConvert)
      val ListOfMapOfPipeLineConfigValue = List(pipeLineConfigValueConvert)
      val keys = ListOfMapOfPipeLineConfigValue.flatMap(_.keys)
      val values = ListOfMapOfPipeLineConfigValue.flatMap(_.values)

      (keys,values)
    }

    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to empty string and then converted to Map[String, String]().To Overide,add in application config file for $applicationConstant as comma separated attributes")

        (List(""),List(""))
    }

    (pipeLineConfigValue._1,pipeLineConfigValue._2)

  }

  /**
   * Read Json like string value from PipeLineConfig object and converts to Seq[(String, String)].If not defined in conf file then will default to Seq.empty[(String, String)]
   * like \"{\"Revenue\":\"revenue_a\", \"Salecost\":\"decimal(38,2)\"}\" ") and converts to sequence of string tuple
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return converted pipeLineConfigValue
   */

  def getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig: PipeLineConfig, applicationConstant: String): Seq[(String, String)]
  = {
    val pipeLineConfigValue: Seq[(String, String)] = try {
      val pipeLineConfigValue = pipeLineConfig.config.getString(applicationConstant)
      val pipeLineConfigValueConvert = convertJsonStringToListOfStringTuple(pipeLineConfigValue)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue + "  which is converted to Seq[(String, String)] as :" + pipeLineConfigValueConvert)

      pipeLineConfigValueConvert
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to empty string.Should be provided as Json.To Overide,add in application config file for $applicationConstant " +
        "like validateDataTypeColumns = \"{\"Revenue\":\"revenue_a\", \"Salecost\":\"decimal(38,2)\"}\" ")
        Seq.empty[(String, String)]
    }
    pipeLineConfigValue

  }

  /**
   * Get string value from PipeLineConfig object.If not defined in conf file then will default to empty string
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return pipeLineConfigValue
   */

  def getStringValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, applicationConstant: String,default:String=""): String
  = {
    val pipeLineConfigValue: String = try {
      val pipeLineConfigValue = pipeLineConfig.config.getString(applicationConstant)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue)

      pipeLineConfigValue
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to empty string.To Overide,add in application config file for $applicationConstant")

        default
    }
    pipeLineConfigValue

  }

  /**
   * Get boolean value from PipeLineConfig object.If not defined in conf file then will default to false
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return pipeLineConfigValue
   */

  def getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, applicationConstant: String,default:Boolean=false): Boolean
  = {
    val pipeLineConfigValue: Boolean = try {
      val pipeLineConfigValue = pipeLineConfig.config.getBoolean(applicationConstant)
      log.info(s"$applicationConstant configured is  :" + pipeLineConfigValue)

      pipeLineConfigValue
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to false.To Overide,add in application config file for $applicationConstant")

        default
    }
    pipeLineConfigValue

  }

  /**
   * Get int value from PipeLineConfig object.If not defined in conf file then will default to 0
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return pipeLineConfigValue
   */

  def getIntValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, applicationConstant: String,default:Int=0): Int
  = {
    val pipeLineConfigValue: Int = try {
      val pipeLineConfigValue = pipeLineConfig.config.getInt(applicationConstant)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue)

      pipeLineConfigValue
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to 0.To Overide,add in application config file for $applicationConstant")

        default
    }
    pipeLineConfigValue

  }

  /**
   * Get long value from PipeLineConfig object.If not defined in conf file then will default to 0
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return pipeLineConfigValue
   */

  def getLongValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, applicationConstant: String,default:Long=0): Long
  = {
    val pipeLineConfigValue: Long = try {
      val pipeLineConfigValue:Long = pipeLineConfig.config.getLong(applicationConstant)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue)

      pipeLineConfigValue
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to 0.To Overide,add in application config file for $applicationConstant")

        default
    }
    pipeLineConfigValue

  }

  /**
   * Get double value from PipeLineConfig object. If not defined in conf file then will default to 0
   *
   * @param pipeLineConfig      PipeLineConfig
   * @param applicationConstant application constant value(like app.xxxxxxxxxxx)
   * @return pipeLineConfigValue
   */

  def getDoubleValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, applicationConstant: String,default:Int=0): Double
  = {
    val pipeLineConfigValue: Double = try {
      val pipeLineConfigValue = pipeLineConfig.config.getDouble(applicationConstant)
      log.info(s"$applicationConstant config value is  :" + pipeLineConfigValue)

      pipeLineConfigValue
    }
    catch {
      case _: ConfigException => log.info(s"$applicationConstant defaulted to 0.To Overide,add in application config file for $applicationConstant")

        default
    }
    pipeLineConfigValue

  }

  /**
   * Add partition Date column by converted sourceColumnName to date using to_date and mapped to target column name
   *
   * @param inputDF          inputDF
   * @param sourceColumnName source column name is converted to to_date and mapped to target column
   * @param targetColumnName targetColumnName
   * @return DataFrame
   */

  def addDatePartitionColumn(inputDF: DataFrame,
                             isPartitionColumnDatatypeDate: Boolean,
                             sourceColumnName: String,
                             targetColumnName: String)
                            (implicit sparkSession: SparkSession): DataFrame = {

    //if date then to_date else no to date
    val partitionColumnTransformation = if (isPartitionColumnDatatypeDate) {
      to_date(col(sourceColumnName))
    } else {
      to_date(col(sourceColumnName)).cast("String")
    }

    val addPartitionColumnDF = inputDF.withColumn(targetColumnName, partitionColumnTransformation)

    addPartitionColumnDF

  }

  /**
   * Convert comma separated strings to Dataframe columns
   *
   * @param str comma separated strings
   * @return DataFrame with columns from comma separated strings
   */

  def stringToDataFrameColumns(str: String)(implicit sparkSession: SparkSession): DataFrame = {

    val strTrim = str.trim.split(",").map(_.trim).mkString(",")
    val schema = StructType(strTrim.trim.split(",").map(column => StructField(column, StringType, true)))
    val executionOrderDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    executionOrderDF
  }


  /**
   * Will pick the DataFrame from the list of Dataframes/transformation name  provided based on the input name of dataframe/transformation name  and order of execution of Dataframes/transformation name
   *
   * @param DFOrderOfExecution Order of Names of Dataframes/transformation name  executed
   * @param nameOfDFToPick1    1st name of Dataframes/transformation name  to pick
   * @param nameOfDFToPick2    2nd name of Dataframes/transformation name  to pick
   * @param listOfDF           list of Dataframes that are executed based on the DFOrderOfExecution
   * @return DataFrame
   */

  def pickDFFromListOfDFBasedOnOrderOfExecution(DFOrderOfExecution: String, nameOfDFToPick1: String, nameOfDFToPick2: String, listOfDF: mutable.Seq[DataFrame]): (DataFrame, DataFrame) = {

    val executionOrder: Array[String] = convertStringToArray(DFOrderOfExecution)
    //val indexOfSpecificDataFrame = executionOrder.indexOf(nameOfDFToPick1)
    //log.info(s"Index Of DataFrame $nameOfDFToPick1 is $indexOfSpecificDataFrame from the executionOrder list $DFOrderOfExecution  ==>" + indexOfSpecificDataFrame)
    val indexOfSpecificDataFrame1: Int = try {
      val indexOfSpecificDataFrame = executionOrder.indexOf(nameOfDFToPick1)
      log.info(s"Index Of DataFrame/transformation name  $nameOfDFToPick1 is $indexOfSpecificDataFrame from the executionOrder list $DFOrderOfExecution  ==>" + indexOfSpecificDataFrame +
        "This Index points to input DataFrame/transformation name and is used to pick from the input List Of Dataframes")
      indexOfSpecificDataFrame
    } catch {
      case e: ArrayIndexOutOfBoundsException => throw ArrayIndexOutOfBoundsException(s"$nameOfDFToPick1 is not in the list of DF $listOfDF that is executed=>" + e)
    }

    //val indexOfLastTransformedDataFrame = executionOrder.indexOf(nameOfDFToPick2)
    //log.info(s"Index Of  DataFrame $nameOfDFToPick2 is $indexOfLastTransformedDataFrame from the executionOrder list $DFOrderOfExecution ==>" + indexOfLastTransformedDataFrame)
    val indexOfSpecificDataFrame2: Int = try {
      val indexOfSpecificDataFrame2 = executionOrder.indexOf(nameOfDFToPick2)
      log.info(s"Index Of  DataFrame/transformation name  $nameOfDFToPick2 is $indexOfSpecificDataFrame2 from the executionOrder list $DFOrderOfExecution ==>" + indexOfSpecificDataFrame2 +
        "This Index points to input DataFrame/transformation name and is used to pick from the input List Of Dataframes")
      indexOfSpecificDataFrame2
    } catch {
      case e: ArrayIndexOutOfBoundsException => throw ArrayIndexOutOfBoundsException(s"$nameOfDFToPick2 is not in the list of DF $listOfDF that is executed=>" + e)
    }
    val nameOfDFToPick1DF = listOfDF(indexOfSpecificDataFrame1)
    val nameOfDFToPick2DF = listOfDF(indexOfSpecificDataFrame2)

    (nameOfDFToPick1DF, nameOfDFToPick2DF)
  }


  /**
   * Will show dataframe result
   *
   * @param inputDF          inputDF
   * @param showResultsToLog Boolean.If set will show result
   * @param logMsg           logMsg
   * @param numOfRows        numOfRows to show.Defaulted to 10
   * @return Unit
   */

  def showResultsToLog(inputDF: DataFrame, showResultsToLog: Boolean, logMsg: String, numOfRows: Int = 10): Unit = {
    if (showResultsToLog) {
      log.info(logMsg)
      inputDF.show(numOfRows, false)
    }
  }

  /**
   * Will show dataframe result with selected columns
   *
   * @param inputDF          inputDF
   * @param showResultsToLog Boolean.If set will show result
   * @param logMsg           logMsg
   * @param numOfRows        numOfRows to show
   * @return Unit
   */

  def showResultsToLog(inputDF: DataFrame, selectColumns: Seq[String], showResultsToLog: Boolean, logMsg: String, numOfRows: Int): Unit = {
    if (showResultsToLog) {
      log.info(logMsg)
      inputDF.select(selectColumns.map(c => col(c)): _*).show(numOfRows, false)
    }
  }

  /**
   * Will pick the DataFrame from the list of Dataframes provided based on the  order of execution of Dataframes
   * and input name of dataframe/transformation name and execute sql against the dataframe
   *
   * @param DFOrderOfExecution                      Order of Names of Dataframes executed
   * @param nameOfDFToPickAndExecuteSqlAndShowToLog Json string representation of name of Dataframes/transformation name to pick and sql to be executed against the dataframe
   *                                                like { \"readInput\":\"select * from readInput\"   ,\"rename\":\"select * from rename\"} in config file is transformed to
   *                                                Seq[(String,String] and provided as input here like Seq(("readInput","select * from readInput"),("rename","select * from rename"))
   * @param listOfDF                                list of Dataframes that are executed based on the DFOrderOfExecution
   * @param numOfRows                               numOfRows to show.Defaulted to 10
   * @return DataFrame
   */

  def showCustomResultsToLog(DFOrderOfExecution: String,
                             nameOfDFToPickAndExecuteSqlAndShowToLog: Seq[(String, String)],
                             listOfDF: mutable.Seq[DataFrame],
                             numOfRows: Int = 10
                            )(implicit sparkSession: SparkSession): Unit = {
    nameOfDFToPickAndExecuteSqlAndShowToLog.foreach(dataFrameNameAndSql => {
      val dataFrameNameToBePickedFromListOfDFBasedOnOrderOfExecutionDF = dataFrameNameAndSql._1
      val sql = dataFrameNameAndSql._2
      val dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF = pickDFFromListOfDFBasedOnOrderOfExecution(DFOrderOfExecution, dataFrameNameToBePickedFromListOfDFBasedOnOrderOfExecutionDF, dataFrameNameToBePickedFromListOfDFBasedOnOrderOfExecutionDF, listOfDF)._1
      dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF.createOrReplaceTempView(dataFrameNameToBePickedFromListOfDFBasedOnOrderOfExecutionDF)
      val executeSqlDF = readExternalInputTableToDF(sql, 2, 3 * 1000)
      val msg = s"Showing results for the DataFrame==>$dataFrameNameToBePickedFromListOfDFBasedOnOrderOfExecutionDF after executing sql==>$sql"
      showResultsToLog(executeSqlDF, true, msg, numOfRows)
    })
  }


  /**
   * Will pick the DataFrame from the list of Dataframes provided based on the  order of execution of Dataframes
   * and input name of dataframe/transformation name and execute sql against the dataframe
   *
   * @param DFOrderOfExecution Order of Names of Dataframes executed
   * @param DFNamesUsedInSql   Sequence of Name of Dataframes/transformation name
   *                           like "readInput,rename" in config file is transformed to
   *                           Seq[String] and provided as input here like Seq("readInput",rename)
   * @param inputSql           input sql with transformation names in the from sql
   * @param listOfDF           list of Dataframes that are executed based on the DFOrderOfExecution
   * @return DataFrame
   */

  def executeExternalSqlOld(DFOrderOfExecution: String,
                            DFNamesUsedInSql: Seq[String],
                            inputSql: String,
                            listOfDF: mutable.Seq[DataFrame])(implicit sparkSession: SparkSession): DataFrame = {
    DFNamesUsedInSql.foreach(dataFrameName => {
      val dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF = pickDFFromListOfDFBasedOnOrderOfExecution(DFOrderOfExecution, dataFrameName, dataFrameName, listOfDF)._1
      dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF.createOrReplaceTempView(dataFrameName)
    })

    val executeSqlDF = readExternalInputTableToDF(inputSql, 2, 3 * 1000)
    executeSqlDF
  }

  /**
   * This will pick DataFrame from the list of Dataframes provided based on the  order of execution of Dataframes
   * and execute sql against the dataframe
   *
   * @param DFOrderOfExecution Order of Names of Dataframes executed
   * @param DFNamesUsedInSql   Sequence of Name of Dataframes/transformation name
   *                           like "readInput,rename" in config file is transformed to
   *                           Seq[String] and provided as input here like Seq("readInput",rename) through executeExternalSqlWrapper Method
   * @param inputSql           input sql with transformation names in the from sql provided as """{ \"1\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from replaceNullWithDefaultForAmtColumn group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"   ,
                                                  \"2\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from addFileDate group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"
                                    }"""
   * @param listOfDF           list of Dataframes that are executed based on the DFOrderOfExecution
   * @return  List of DataFrame after executing sql
   */

  def executeExternalSql(DFOrderOfExecution: String,
                         DFNamesUsedInSql: Seq[String],
                         inputSql: Seq[(String,String)],
                         listOfDF: mutable.Seq[DataFrame])(implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {
    DFNamesUsedInSql.foreach(dataFrameName => {
      val dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF = pickDFFromListOfDFBasedOnOrderOfExecution(DFOrderOfExecution, dataFrameName, dataFrameName, listOfDF)._1
      dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF.createOrReplaceTempView(dataFrameName)
    })

    val listOfOutputDF = ArrayBuffer[DataFrame]()

    println(inputSql)

    inputSql.foreach(sql => { //val newSql = sql.replaceAll("\"","")
      val executeSqlDF = readExternalInputTableToDF(sql._2, 2, 3 * 1000)
      listOfOutputDF += executeSqlDF
      showResultsToLog(executeSqlDF, true, s"Showing sample rows of sql $sql", 25)
    })

    listOfOutputDF

  }

  /**
   * Will execute sql against external table and outputs list of dataframes after executing sql
   * @param inputSql           input sql referencing table provided as"""{ \"1\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from prd_mdf_fnd.impact_radius_api_order_tcin group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"   ,
                                                  \"2\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from prd_mdf_fnd.impact_radius_api_order_tcin group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"
                                    }"""
   * @param env                                             env
   * @param selectColumns       Columns to be selected from the table.This is needed for local testing

   * @return  List of DataFrame after executing sql
   */

  def executeSqlAgainstTable(inputSql: Seq[(String,String)],
                             env:String,
                             selectColumns:String
                            )(implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {


    val listOfOutputDF = ArrayBuffer[DataFrame]()

    if (env != "local") {
      inputSql.foreach(sql => { //val newSql = sql.replaceAll("\"","")
        val executeSqlDF = readExternalInputTableToDF(sql._2, 2, 3 * 1000)
        listOfOutputDF += executeSqlDF
        showResultsToLog(executeSqlDF, true, s"Showing sample rows of sql $sql from executeSqlAgainstTable action", 25)
      })

      listOfOutputDF
    } else {
      val selectTrimColumns = selectColumns.trim.split(",").map(_.trim).mkString(",")
      val schema = StructType(selectTrimColumns.trim.split(",").map(column => StructField(column, StringType, true)))
      val executeSqlDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
      listOfOutputDF += executeSqlDF
      showResultsToLog(executeSqlDF, true, s"Showing sample rows from executeSqlAgainstTable action", 25)
      listOfOutputDF
    }

  }

  /**
   * This will pick DataFrame from the list of Dataframes provided based on the  order of execution of Dataframes
   * and execute sql against the dataframe
   *
   * @param DFOrderOfExecution Dataframes Names used in SQL matching with listOfDF.If sql use ABC and QWE as dataframes then provide "ABC,QWE" and listOfDF should have dataframes corresponding to "ABC,QWE"  and should be in this order
   * @param listOfDF           list of Dataframes that are in inputSql.Like dataframes corresponding to "ABC,QWE"  and should be in this order
   * @param DFNamesUsedInSql   Dataframe names used in inputSql.Like Seq("ABC","QWE")
   * @param inputSql           input sql with transformation names in the from sql provided as """{ \"1\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from ABC group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"   ,
                                                  \"2\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from QWE group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"
                                    }"""
   * @return  List of DataFrame after executing sql
   */

  def executeSqlAgainstDataframe(DFOrderOfExecution: String,
                                 listOfDF: mutable.Seq[DataFrame],
                                 DFNamesUsedInSql: Seq[String],
                                 inputSql: Seq[(String,String)]
                                )(implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {
    DFNamesUsedInSql.foreach(dataFrameName => {
      val dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF = pickDFFromListOfDFBasedOnOrderOfExecution(DFOrderOfExecution, dataFrameName, dataFrameName, listOfDF)._1
      dataFramePickedFromListOfDFBasedOnOrderOfExecutionDF.createOrReplaceTempView(dataFrameName)
    })

    val listOfOutputDF = ArrayBuffer[DataFrame]()

    println(inputSql)

    inputSql.foreach(sql => { //val newSql = sql.replaceAll("\"","")
      val executeSqlDF = readExternalInputTableToDF(sql._2, 2, 3 * 1000)
      listOfOutputDF += executeSqlDF
      showResultsToLog(executeSqlDF, true, s"Showing sample rows of sql $sql", 25)
    })

    listOfOutputDF

  }

  /**
   * This will join calendar table
   *
   * @param inputDF inputDF
   * @param inputJoinConditionColumn                       input  column used to join calendar table
   * @param calendarColumns                                Columns to be selected from calendar table.If calendarColumnsWithAnyTransformation is specified then this could be empty string.
   *                                                       Specify this even when specifying calendarColumnsWithAnyTransformation when running it local same as calendarColumnsWithAnyTransformation but only columns no transformation.
   * @param calendarColumnsWithAnyTransformation           Columns to be selected from calendar table with any transformation.This takes precedence over calendarColumns.
   *                                                       If calendarColumns is specified then this could be empty string
   * @param calendarTable                                  calendarTable with schema name
   * @param calendarJoinConditionColumn                    calendar table join column
   * @param tempCalendarExtLocation                        hdfs location for 2 step read
   * @param jdbcUrl                                        jdbc for 2 step read
   * @param queueName                                       queueName for 2 step read
   * @param env                                             env

   * @return  DataFrame
   */

  def joinCal(inputDF: DataFrame,
              inputJoinConditionColumn:String,
              calendarColumns:String,
              calendarColumnsWithAnyTransformation:String,
              calendarTable:String,
              calendarJoinConditionColumn:String,
              tempCalendarExtLocation:String,
              jdbcUrl:String,
              queueName:String,
              env:String
             )(implicit sparkSession: SparkSession): DataFrame = {


    log.info(s"Get distinct dates from the input based on ${inputJoinConditionColumn}")
    val listOfDatesInInput: String = getDistinctInputDates(inputDF, inputJoinConditionColumn)

    log.info("listOfFileDatesInInput=" + listOfDatesInInput)

    val calendarTrimColumns = calendarColumns.trim.split(",").map(_.trim).mkString(",")
    val calendarTrimColumnsWithAnyTransformation = calendarColumnsWithAnyTransformation.trim.split(",").map(_.trim).mkString(",")

    val selectColumns = if (calendarTrimColumnsWithAnyTransformation != "") {
      calendarTrimColumnsWithAnyTransformation
    } else {
      calendarTrimColumns
    }
    val whereClause = s"where calendar_d in ($listOfDatesInInput)"

    val calendarQuery = s"SELECT ${selectColumns}  from ${calendarTable} ${whereClause}"

    val calendarDF: DataFrame = if (env != "local") {
      readFromManagedTableWith2StepProcess(jdbcUrl,queueName, tempCalendarExtLocation, calendarQuery, 2)

    } else {
      val schema = StructType(calendarTrimColumns.trim.split(",").map(column => StructField(column, StringType, true)))
      sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

    }

    val joinCalDF = DataFrameUtils.joinTwoDataFrames(inputDF,
      calendarDF,
      "left",
      s"${inputJoinConditionColumn}=${calendarJoinConditionColumn}",
      prefixRightDFCommonCol = "right")._1

    joinCalDF
  }

  /**
   * Wrapper for 2 step read for managed table
   * @param jdbcURL                                         jdbc for 2 step read
   * @param queueName                                       queueName for 2 step read
   * @param hdfsLocation                                    hdfsLocation
   * @param inputQuery                                      inputQuery
   * @param retryCount                                      retryCount
   * @return  DataFrame
   */
  def readFromManagedTableWith2StepProcess(jdbcURL: String,
                                           queueName:String,
                                           hdfsLocation: String,
                                           inputQuery: String,
                                           retryCount: Int)(implicit spark: SparkSession): DataFrame = {
    val properties = new Properties()
    properties.put("hive.execution.engine", "tez")
    properties.put("tez.queue.name", queueName)
    properties.put("hive.auto.convert.join", "true")
    properties.put("hive.auto.convert.join.noconditionaltask", "true")
    properties.put("hive.execution.engine", "tez")
    properties.put("hive.merge.smallfiles.avgsize", "1073741824")
    properties.put("hive.merge.size.per.task", "4294967296")
    properties.put("hive.merge.mapredfiles", "true")
    properties.put("hive.merge.mapfiles", "true")
    properties.put("hive.merge.tezfiles", "true")
    properties.put("hive.tez.container.size", "10240")
    properties.put("hive.tez.java.opts", "-Xmx8192m")
    properties.put("tez.am.resource.memory.mb", "10240")
    properties.put("hive.cbo.enable", "true")
    properties.put("hive.stats.autogather", "true")
    properties.put("hive.tez.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat")
    properties.put("hive.optimize.sort.dynamic.partition", "true")
    val hiveTableReadConfigDetails = new HiveTableReadConfigDetails(inputQuery, hdfsLocation, jdbcURL, properties)
    val outputDF = readFromHiveManagedTableWith2StepProcess(hiveTableReadConfigDetails, retryCount)

    outputDF
  }

  /**
   * Drops dups based on dupCheckKey and stores in dups in nullDupOutputLoc.If dupCheckKey is empty then will remove on all columns
   *
   * @param inputDF          inputDF
   * @param nullDupOutputLoc dups location or path where it will be stored for reference
   * @param dupCheckKey      Sequence of dup keys
   * @param orderByCol       order by column
   * @return DataFrame
   */

  def dropDup(inputDF: DataFrame, nullDupOutputLoc: String, dupCheckKey: Seq[String], orderByCol: String
             ): DataFrame = {

    if (dupCheckKey.isEmpty) {
      val removeDupDF = MessageCleanser.removeDuplicates(inputDF)
      removeDupDF
    } else {


      val pkColumns = dupCheckKey.map(col)
      val windowSpec = Window.partitionBy(pkColumns: _*).orderBy(orderByCol)
      val rowNumberDF = inputDF.withColumn("row_number", row_number.over(windowSpec))
      val rowNumberDupDF = rowNumberDF.filter("row_number > 1")
      val rowNumberDupCount = rowNumberDupDF.count()
      log.info(s"Dup count in source dataset based on key $dupCheckKey=>$rowNumberDupCount")
      if (rowNumberDupCount > 0) {
        log.info(s"Writing dup checked on keys $dupCheckKey in the location for reference=>$nullDupOutputLoc")
        FileWriterService.writeDQCheckData(rowNumberDupDF, nullDupOutputLoc)
      }

      val rowNumberRemoveDupDF = rowNumberDF.filter("row_number = 1")
      rowNumberRemoveDupDF
    }

  }

  /**
   * Deletes the touch file which is required to be present prior to executing the pipeline.
   *
   * @param doneFile Done file path
   */
  def deleteTriggerFile(doneFile: String)(implicit sparkSession: SparkSession): Unit = {
    //Deletes the touch file which is required to be present prior to executing the pipeline.
    FileWriterService.deleteTouchFile(doneFile)
  }

  /**
   * @param inputDF      The starting DataFrame
   * @param inputColumns The Sequence of column names which are in the inputDF and that will be converted to utc timestamp
   * @param inputFormat  input data format.Defaulted to "yyyy-MM-dd'T'HH:mm:ss.SSS"
   * @param outputFormat output data format.Defaulted to "yyyy-MM-dd HH:mm:ss"
   * @param timezone     input column timezone etc "America/Chicago"
   * @return A new DataFrame that has each of the columns in the columns sequence converted to utc timestamp
   */
  def convertTimestampFromLocalToUtc(inputDF: DataFrame,
                                     inputColumns: Seq[String],
                                     inputFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSS",
                                     outputFormat: String = "yyyy-MM-dd HH:mm:ss",
                                     timezone: String = "America/Chicago"): DataFrame = {
    val timestampConvertedDF: DataFrame = {
      inputColumns.foldLeft(inputDF)(
        (tempDF, x) =>
          tempDF.withColumn(x, date_format(to_utc_timestamp(to_timestamp(
            col(x), inputFormat), timezone), outputFormat)
          )
      )
    }
    timestampConvertedDF
  }


  /**
   * @param inputDF      The starting DataFrame
   * @param inputColumns The Sequence of column names which are in the inputDF and that will be converted to utc timestamp
   * @param inputFormat  input column date format.Defaulted to "yyyy-MM-dd'T'HH:mm:ss.SSS"
   * @param outputFormat output  column date format.Defaulted to "yyyy-MM-dd HH:mm:ss"
   * @param timezone     output column timezone etc "America/Chicago"
   * @return A new DataFrame that has each of the columns in the columns sequence converted to local timestamp
   */
  def convertTimestampFromUtcToLocal(inputDF: DataFrame,
                                     inputColumns: Seq[String],
                                     inputFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSS",
                                     outputFormat: String = "yyyy-MM-dd HH:mm:ss",
                                     timezone: String = "America/Chicago"): DataFrame = {
    val timestampConvertedDF: DataFrame = {
      inputColumns.foldLeft(inputDF)(
        (tempDF, x) =>
          tempDF.withColumn(x, date_format(from_utc_timestamp(to_timestamp(
            col(x), inputFormat), timezone), outputFormat)
          )
      )
    }
    timestampConvertedDF
  }

  /**
   * @param inputDF      The starting DataFrame
   * @param inputColumns The Sequence of column names which are in the inputDF and that will be converted to Date
   * @return A new DataFrame that has each of the columns in the columns sequence converted to Date
   */
  def convertTimestampToDate(inputDF: DataFrame,
                             inputColumns: Seq[String],
                             outputFormat: String = "yyyy-MM-dd"): DataFrame = {
    val timestampConvertedDF: DataFrame = {
      inputColumns.foldLeft(inputDF)(
        (tempDF, x) =>
          tempDF.withColumn(x, date_format(to_date(
            col(x)), outputFormat))
      )
    }
    timestampConvertedDF
  }

  /**
   * @param inputDF                 The starting DataFrame
   * @param inputColumnsWithDefault Input column name with default value provided as a Sequence of string tuple
   * @return DataFrame
   */
  def handleNullsInPKColumns(inputDF: DataFrame,
                             inputColumnsWithDefault: Seq[(String, String)]): DataFrame = {


    val outputDF = inputColumnsWithDefault.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.na.fill(colName._2, Array(colName._1))
    }
    outputDF
  }

  /**
   * If input dataframe count is zero and if record is kafka(set by ifKafkaThenHandleEmptyTopic) then will get previous day's records from the table based on partition column
   * If input dataframe count is zero and if record is not kafka(not set by ifKafkaThenHandleEmptyTopic) and if abortOnZeroRecords is set then will throw GeneralException
   * else  throws ZeroRecordInDataframeException and program calling this can catch this and move forward with execution in case of not aborting the flow
   * @param inputDF                      The input DataFrame
   * @param DataframeNamePassedForLogMsg Provide name of Dataframe or transformation/ActionName to be shown in the log message
   * @param abortOnZeroRecords           Defaulted to false.If set will throw ZeroRecordInDataframeException exception
   * @param ifKafkaThenHandleEmptyTopic  If set then indicates its a kafka record and will get previous day's records from the table based on partition column if input dataframe count is zero
   * @param fndTablePartitionColumn      Used when handleEmptyTopic is true to get previous day's records from the table based on partition column
   * @param commonPipeLineHelper         CommonPipeLineHelper type

   * @return DataFrame
   */

  def handleZeroRecordInDataframe(inputDF: DataFrame,
                                  DataframeNamePassedForLogMsg: String,
                                  abortOnZeroRecords: Boolean = false,
                                  ifKafkaThenHandleEmptyTopic: Boolean = false,
                                  fndTablePartitionColumn: String = "",
                                  commonPipeLineHelper: CommonPipeLineHelper
                                 )(implicit sparkSession: SparkSession): DataFrame = {

    if (inputDF.count == 0) {
      val finalFndDF: DataFrame = if (ifKafkaThenHandleEmptyTopic) {
        handleEmptyTopic(commonPipeLineHelper.env,
          commonPipeLineHelper.fndDb,
          commonPipeLineHelper.fndTable,
          commonPipeLineHelper.queueName,
          commonPipeLineHelper.jdbcURL,
          fndTablePartitionColumn)(sparkSession)
      } else if (abortOnZeroRecords) {
        throw GeneralException(s"*******************There are zero records in the DataFrame $DataframeNamePassedForLogMsg********************************")

      } else {

        throw ZeroRecordInDataframeException(s"*******************There are zero records in the DataFrame $DataframeNamePassedForLogMsg********************************")

      }
      finalFndDF
    } else {
      inputDF
    }
  }

  /**
   * Adds the inputDF to a input list and calls method handleZeroRecordInDataframe
   * (If input dataframe count is zero and if abortOnZeroRecords is set then will throw GeneralException
   * else  throws ZeroRecordInDataframeException and program calling this can catch this and move forward with execution in case of not aborting the flow)
   * And show results to log if app.showResultsToLog.showResultsToLog is set in the config file
   * @param listOfDF                     list of Dataframes
   * @param inputDF                      inputDF
   * @param DataframeNamePassedForLogMsg Provide name of Dataframe or transformation/ActionName to be shown in the log message
   * @param abortOnZeroRecords           Defaulted to false.If set will throw ZeroRecordInDataframeException exception
   * @param commonPipeLineHelper         CommonPipeLineHelper type
   * @param showResultsToLogObject       ShowResultsToLog type

   * @return listOfDF
   */

  def addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF: ArrayBuffer[DataFrame],
                                                         inputDF: DataFrame,
                                                         DataframeNamePassedForLogMsg: String,
                                                         abortOnZeroRecords: Boolean,
                                                         commonPipeLineHelper: CommonExecutePipelineParameterObjects.CommonPipeLineHelper,
                                                         showResultsToLogObject: CommonExecutePipelineParameterObjects.ShowResultsToLog)
                                                        (implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {
    listOfDF += inputDF
    handleZeroRecordInDataframe(inputDF, DataframeNamePassedForLogMsg, abortOnZeroRecords, commonPipeLineHelper = commonPipeLineHelper)
    showResultsToLog(inputDF, showResultsToLogObject.showResultsToLog, s"Showing sample rows of the dataframe $DataframeNamePassedForLogMsg", showResultsToLogObject.numOfRows)

    listOfDF
  }

  /**
   * Adds the inputDF to a input list
   * And show results to log if app.showResultsToLog.showResultsToLog is set in the config file
   * @param listOfDF                     list of Dataframes
   * @param inputDF                      inputDF
   * @param DataframeNamePassedForLogMsg Provide name of Dataframe or transformation/ActionName to be shown in the log message
   * @param showResultsToLogObject       ShowResultsToLog type

   * @return listOfDF
   */

  def addDataframeToListAndShowResult(listOfDF: ArrayBuffer[DataFrame],
                                      inputDF: DataFrame,
                                      DataframeNamePassedForLogMsg: String,
                                      commonPipeLineHelper: CommonExecutePipelineParameterObjects.CommonPipeLineHelper,
                                      showResultsToLogObject: CommonExecutePipelineParameterObjects.ShowResultsToLog)
                                     (implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {
    listOfDF += inputDF
    showResultsToLog(inputDF, showResultsToLogObject.showResultsToLog, s"Showing sample rows of the dataframe $DataframeNamePassedForLogMsg", showResultsToLogObject.numOfRows)

    listOfDF
  }

  /**
   * Adds the inputDF to a input list and calls method handleZeroRecordInDataframe
   * (If input dataframe count is zero and if record is kafka(set by ifKafkaThenHandleEmptyTopic) then will get previous day's records from the table based on partition column
   * If input dataframe count is zero and if record is not kafka(not set by ifKafkaThenHandleEmptyTopic) and if abortOnZeroRecords is set then will throw GeneralException
   * else  throws ZeroRecordInDataframeException and program calling this can catch this and move forward with execution in case of not aborting the flow)
   * And show results to log if app.showResultsToLog.showResultsToLog is set in the config file
   * @param listOfDF                     list of Dataframes
   * @param inputDF                      inputDF
   * @param DataframeNamePassedForLogMsg Provide name of Dataframe or transformation/ActionName to be shown in the log message
   * @param abortOnZeroRecords           Defaulted to false.If set will throw ZeroRecordInDataframeException exception
   * @param commonPipeLineHelper         CommonPipeLineHelper type
   * @param showResultsToLogObject       ShowResultsToLog type

   * @return listOfDF
   */

  def addDataframeToListAndHandleEmptyTopicAndShowResult(listOfDF: ArrayBuffer[DataFrame],
                                                         inputDF: DataFrame,
                                                         DataframeNamePassedForLogMsg: String,
                                                         abortOnZeroRecords: Boolean,
                                                         handleEmptyTopic: Boolean,
                                                         fndTablePartitionColumn: String,
                                                         commonPipeLineHelper: CommonExecutePipelineParameterObjects.CommonPipeLineHelper,
                                                         showResultsToLogObject: CommonExecutePipelineParameterObjects.ShowResultsToLog)
                                                        (implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {
    listOfDF += inputDF
    val outputDF = handleZeroRecordInDataframe(inputDF, DataframeNamePassedForLogMsg, abortOnZeroRecords, handleEmptyTopic, fndTablePartitionColumn, commonPipeLineHelper)
    showResultsToLog(outputDF, showResultsToLogObject.showResultsToLog, s"Showing sample rows of the dataframe $DataframeNamePassedForLogMsg", showResultsToLogObject.numOfRows)

    listOfDF

  }

  /**
   * deletes lock and hidden from when testing local
   * And show results to log if app.showResultsToLog.showResultsToLog is set in the config file
   * @param config                          MarketingPipelineConfigs

   * @return Unit
   */

  def deleteLockAndHiddenFileFromLocal(config: MarketingPipelineConfigs, initializeCommonPipeLineHelper: CommonPipeLineHelper)
                                      (implicit sparkSession: SparkSession): Unit
  = {
    if (initializeCommonPipeLineHelper.env == "local") {
      deleteLock(MarketingCommonUtils.appendSlashInEnd(config.pipeLineConfig.config.getString(MarketingConstants.APP_LOCKLOCATION)), config.appName)
      deleteHiddenFile(initializeCommonPipeLineHelper.inputEphemeralPath)
    }
  }

  /**
   * This calls dpp common EmailService.sendEmailWrapper.This method is to send an email to the given address by attaching the input dataframe.
   * If attachDFResult and attachDFResultToEmailBody is set to false then DF will not be attached but still
   * needs to provide a dummy dataframe as inputDF
   * @param inputDF                    Dataframe to be attached in email
   * @param env                        env
   * @param customSqlFlag              Boolean.If set then will execute customSql against inputDF and result is send to email
   * @param customSql                  If customSqlFlag is set then will execute customSql against inputDF and result is send to email
   * @param fromAddress                From Address
   * @param toAddress                  To Address
   * @param reportFilename             Report Filename with extension(timestamp will be added to the filename) attached to the email.If empty string then will not attach the file.Date will added to filename and csv file extension will also be added
   * @param emailSubject               Email Subject line.Date will added to Subject line
   * @param emailBodySubject           Subject line in the message body.Date will added to Subject line
   * @param attachDFResult             Boolean.If set then will attach DF as a file to the email
   * @param attachDFResultToEmailBody  Boolean.If set then will attach DF to the email body in html format
   * @param debug  String.If set then will Debug mode for email service is turned on

   * @return - Unit
   */

  def sendDFToEmail(inputDF: DataFrame,
                    env:String,
                    customSqlFlag: Boolean,
                    customSql: String,
                    fromAddress: String,
                    toAddress: String,
                    reportFilename:String,
                    emailSubject:String,
                    emailBodySubject:String,
                    attachDFResult:Boolean,
                    attachDFResultToEmailBody:Boolean,
                    debug:String
                   )(implicit sparkSession: SparkSession): Unit = {

    if (env != "local") {

      val  reportFilenameNoExt = getFileNameAndExtensionFromFileName(reportFilename)._1
      val  reportFilenameExt = getFileNameAndExtensionFromFileName(reportFilename)._2
      val  reportFilenameWithDate = s"${reportFilenameNoExt}_${java.time.LocalDate.now.toString}.${reportFilenameExt}"

      sendEmailWrapper(inputDF,
        true,
        customSqlFlag,
        customSql,
        EmailService(
          fromAddress,
          toAddress,
          s"${reportFilenameWithDate}",
          s"${emailSubject}--->${java.time.LocalDate.now.toString}",
          s"${emailBodySubject}--->${java.time.LocalDate.now.toString}",
          attachDFResult,
          attachDFResultToEmailBody,
          debug))
    }
  }

  /**
   * Will save DF to HDFS path
   * @param inputDF                     Dataframe which needs to be saved to HDFS
   * @param selectColumns               select Columns from inputDF
   * @param filterCondition             filter Condition on inputDF
   * @param hdfsTempFileLocation        Temp Hdfs location where the DF will stored.
   * @param hdfsFileLocation            File in hdfsTempFileLocation will be moved to this hdfs location
   *                                    after renaming the file in sftpHdfsTempFileLocation to sftpFilename
   * @param hdfsSaveOptions             Options to be used while saving DF to HDFS.like
   *                                    Map("header"->"true","sep"->",","mapreduce.fileoutputcommitter.marksuccessfuljobs"->"false")
   * @param hdfsSaveMode                Defaulted to overwrite to be used while saving DF to HDFS
   * @param hdfsSaveFileFormat          Defaulted to csv to be used while saving DF to HDFS
   * @param hdfsSaveCoalesce            Defaulted to  1 to be used while saving DF to HDFS
   * @param coalesceInputSplit          Defaulted to  1.If greater than 1 then inputDF will be split by the coalesceInputSplit and saves those many files in hdfs
   * @param hdfsRenameFilename          Defaulted to empty string.Filename with extension.Timestamp will be added to the filename.If set,this filename will be used to rename file in hdfs.If set ,file in hdfsTempFileLocation will be renamed to hdfsRenameFilename in the hdfs
   *                                    If there are multiple files in hdfsTempFileLocation which needs to be renamed while moving to hdfs
   *                                    format of rename will be filename_$i_timestamp.fileextension else will use the filename in  hdfsTempFileLocation
   * @param hdfsRenameFileSearchPattern Defaulted to empty string.Based on the pattern, file in searched in the path hdfsTempFileLocation
   *                                    and this will be renamed before moving to hdfsFileLocation
   * @param deleteHdfsFileSearchPattern Defaulted to *.Based on the pattern file will be deleted from hdfsFileLocation
   *                                    before renaming file in hdfsTempFileLocation to hdfsFileLocation
   * @param deleteHdfsFileRecursive     Defaulted to false.If set,file will be deleted recursively from hdfsFileLocation
   *                                    before renaming file in hdfsTempFileLocation to hdfsFileLocation
   * @return Unit
   */
  @SuppressWarnings(Array("MaxParameters"))
  def saveDFToHdfs(

                    inputDF: DataFrame,
                    selectColumns: Seq[String]=Seq.empty[String],
                    filterCondition: String = "",
                    hdfsTempFileLocation: String,
                    hdfsFileLocation: String,
                    hdfsSaveOptions: Map[String, String],
                    hdfsSaveMode: String = "overwrite",
                    hdfsSaveFileFormat: String = "csv",
                    hdfsSaveCoalesce: Int = 1,
                    coalesceInputSplit:Int=0,
                    hdfsRenameFilename: String="",
                    hdfsRenameFileSearchPattern:String="",
                    deleteHdfsFileSearchPattern:String="*",
                    deleteHdfsFileRecursive:Boolean=false
                  )
                  (implicit sparkSession: SparkSession): Unit = {

    val  hdfsRenameFilenameNoExt = getFileNameAndExtensionFromFileName(hdfsRenameFilename)._1
    val  hdfsRenameFilenameExt = getFileNameAndExtensionFromFileName(hdfsRenameFilename)._2
    val hdfsRenameFilenameWithDate = s"${hdfsRenameFilenameNoExt}_${java.time.LocalDate.now.toString}.${hdfsRenameFilenameExt}"

    val inputCountDF = inputDF.count()
    val coalesce = if(coalesceInputSplit >1){
      calculateFileSplit(inputDF,coalesceInputSplit)
    }
    else {
      hdfsSaveCoalesce
    }
    if (inputCountDF > 0) {
      val filterDF: DataFrame = if (filterCondition.nonEmpty) inputDF.where(filterCondition) else inputDF
      val selectColDF = if (selectColumns.nonEmpty) filterDF.select(selectColumns.map(name => col(name)): _*) else filterDF
      hdfsSaveFromDF(selectColDF, writeMode = hdfsSaveMode, fileFormat = hdfsSaveFileFormat, fileSaveLocation = hdfsTempFileLocation, coalesce = coalesce, options = hdfsSaveOptions)

      if (hdfsRenameFilename != "") {
        log.info(s"Renaming the file in the $hdfsTempFileLocation")
        val hdfsFileRenameList = hdfsFileRename(hdfsTempFileLocation, hdfsFileLocation, hdfsRenameFilenameWithDate, hdfsRenameFileSearchPattern, deleteHdfsFileSearchPattern, deleteHdfsFileRecursive)
        if (hdfsFileRenameList.length >= 1) {
          val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
          log.info(s"File ${hdfsFileLocation}/$hdfsFileRenameList exists in the hdfs")
        } else {
          log.info(s"There are no files in hdfs in the path ${hdfsFileLocation} " +
            s"Check the path $hdfsTempFileLocation for the file created by the input dataframe that is passed." +
            "If yes then there may be issue with renaming file in the directory hdfsFileLocation." +
            "Check the parameters hdfsTempFileLocation, hdfsFileLocation, hdfsRenameFilename, hdfsRenameFileSearchPattern, deleteHdfsFileSearchPattern, deleteHdfsFileRecursive ")
          //throw error?
        }
      }
    }
    else {
      log.error("Input Dataframe is empty,so inputDF is not saved to hdfs")
    }
  }



}