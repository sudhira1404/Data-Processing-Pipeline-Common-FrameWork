package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.constants.DPPMetricsConstants
import com.tgt.dsc.kelsa.datapipeline.core.service.{CompactorService, PipeLineMetadataService}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.cleanse.MessageCleanser
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.SchemaException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**
 * Initialize and use this class when reading Kafka Json files landed by the Kelsa DIP. Required to be passed to the
 * [[FileReader.readAndDedupeKafkaFiles]] method.
 *
 * @param inputEphemeralPath inputEphemeralPath
 * @param templateLocation templateLocation
 * @param checkpointPath checkpointPath
 * @param corruptRecordLocation corruptRecordLocation
 * @param inputEphemeralFileFormat inputEphemeralFileFormat
 * @param numberOfPartitions numberOfPartitions
 * @param env env
 * @param dropColumns any columns to be dropped after reading the ephemeral files
 * @param ephemeralFilePathPrefix Defaulted to "load_date=".Prefix will be prefixed with checkpoint location
 *                                to get checkpoint value which will be used in ephemeral path
 * @param templateFileFormat      Defaulted to json.Format of the template which will be used to generate
 *                                schema from the template provided in the templateLocation
 * @param addKafkaTimestamp Set to true if the DataFrame should add the timestamp from the Kafka message to the final
 *                          DataFrame. When true, two extra fields get added based on the timestamp, one in date datatype,
 *                          the other in timestamp datatype. The names of the two added fields are set by the kafkaDateFieldName
 *                          and the kafkaTimestampFieldName. Defaults to false.
 * @param kafkaDateFieldName This is the name of the date field added derived from the Kafka timestamp. Only provide a value if the
 *                           addKafkaTimestamp = true. Defaults to "report_d"
 * @param kafkaTimestampFieldName This is the name of the timestamp field added derived from the Kafka timestamp. Only provide a value if the
 *                           addKafkaTimestamp = true. Defaults to "hive_load_timestamp"
 * @param kafkaValueFieldRename This is the name of the json string derived from the Kafka value field. Defaults to "temp_json_struct"
 */
final case class ReadKafkaFiles(inputEphemeralPath: String,
                                templateLocation: String,
                                checkpointPath: String,
                                corruptRecordLocation: String,
                                inputEphemeralFileFormat: String = "json",
                                numberOfPartitions: Int,
                                env: String,
                                dropColumns: Seq[String] = Seq.empty,
                                ephemeralFilePathPrefix: String = "load_date=",
                                templateFileFormat: String = "json",
                                addKafkaTimestamp: Boolean = false,
                                kafkaDateFieldName: String = "report_d",
                                kafkaTimestampFieldName: String = "hive_load_timestamp",
                                kafkaValueFieldRename: String = "temp_json_struct"
                               )

/**
 * Convenience methods for reading from Files.
 */

object FileReader extends LoggingTrait {

  /**
   * Common method for initiating a DataFrame from files in a directory.
   *
   * @param fileLocation Required. This is the location of the files to read from.
   * @param fileFormat Required. This is the format of the files (eg. csv, json, avro, etc).
   * @param corruptRecordLocation Optional. This is the location where to write corrupt records to. Defaults to empty.
   *                              If left empty, will not write corrupt records to an alternate location, and will leave
   *                              any corrupt records in the DataFrame.
   * @param templateLocation Optional. This is the location of the template file as the schema to be used when reading files. Defaults to empty.
   *                         If left empty, will attempt to infer the schema from the files read.
   * @param predeterminedSchema Optional. If not using a template for passing a schema, you can pre-define and pass in this StructType
   *                            to be used when reading files as the schema. Defaults to an empty StructType. If left empty, will either
   *                            attempt to infer the schema from the files read, or if the templateLocation is not blank, will
   *                            use the file found there as the schema.
   * @param hasHeader Optional. Used for csv or other text based files to indicate whether a header exists in the data. Defaults to false.
   * @param renameColumns Optional. If provided a Sequence of column names, will rename the columns from the read files to the new names.
   *                      Useful only when a schema has been provided, and the column names are in that specified order. Do not attempt to
   *                      use this when the schema is inferred.
   * @param selectColumns Optional. If provided a Sequence of column names, will select only those column names from the read files.
   * @param filterCondition Optional. If provided a string, will use that string in a sql WHERE clause to filter the records in the final DataFrame.
   * @param delimiter Optional. When dealing with delimited files, this should be provided, eg. "," or "\t".
   * @param options Optional. This is a Map of key,value pairs which are used by Spark when reading the files as options.
   * @param getFileName Optional.Defaulted to false. If set then will return name of the file being read and stores it in new column "filename"
   * @param sparkSession An initialized spark session.
   * @return A DataFrame from the read files.
   */
  @SuppressWarnings(Array("MaxParameters"))
  def readFileToDataFrame(fileLocation: String,
                          fileFormat: String,
                          corruptRecordLocation: String = "",
                          templateLocation: String = "",
                          predeterminedSchema: StructType = new StructType(),
                          hasHeader: Boolean = false,
                          renameColumns: Seq[String] = Seq.empty[String],
                          selectColumns: Seq[String] = Seq.empty[String],
                          filterCondition: String = "",
                          delimiter: String = "",
                          options:Map[String,String] = Map[String,String](),
                          getFileName: Boolean = false
                         )(implicit sparkSession: SparkSession): DataFrame = {
    log.info("File format to read : " + fileFormat)
    log.info("Input file location : " + fileLocation)
    if (corruptRecordLocation.nonEmpty) log.info("Corrupt file location : " + corruptRecordLocation)
    val schema: StructType = initializeSchema(templateLocation, fileFormat, predeterminedSchema)

    val inputDF: DataFrame = initializeDataFrameReader(fileFormat, schema, hasHeader,delimiter,options).load(fileLocation)

    val addFileNameDF: DataFrame =if (getFileName)  inputDF.withColumn("filename", input_file_name()) else inputDF

    val validDF: DataFrame = handleCorruptRecords(addFileNameDF, corruptRecordLocation, fileFormat)

    val renameDF: DataFrame = if (renameColumns.nonEmpty) validDF.toDF(renameColumns: _*) else validDF

    val selectDF: DataFrame = if (selectColumns.nonEmpty) renameDF.select(selectColumns.map(name => col(name)): _*) else renameDF

    val filterDF: DataFrame = if (filterCondition.nonEmpty) selectDF.where(filterCondition) else selectDF

    PipeLineMetadataService.setLongMetricValue(DPPMetricsConstants.INPUTROWSCOUNT, filterDF.count())

    filterDF
  }

  /**
   * Used to find corrupt records in a DataFrame, filter them out, and write them to the specified location.
   * @param inputDF The DataFrame to check for corrupt records in.
   * @param corruptRecordLocation The location to write any corrupt records to.
   * @param format The format of the file to save the corrupted records in.
   * @return A new DataFrame without corrupted records in it.
   */
  def handleCorruptRecords(inputDF: DataFrame, corruptRecordLocation: String, format: String): DataFrame = {
    val validDF: DataFrame = {
      inputDF.printSchema()

      if (inputDF.columns.contains(MarketingConstants._CORRUPT_RECORD)) {
        val cacheDF: DataFrame = inputDF.cache()
        val invalidDF: DataFrame = cacheDF.filter(inputDF(MarketingConstants._CORRUPT_RECORD).isNotNull)
        val outputDF: DataFrame = cacheDF.filter(inputDF(MarketingConstants._CORRUPT_RECORD).isNull)
        val invalidInputMessageCount: Long = invalidDF.count()
        //cacheDF.unpersist()
        if (invalidInputMessageCount > 0) {

          log.error("Corrupted records detected!")

          PipeLineMetadataService.setLongMetricValue(MarketingConstants.INVALIDINPUTMESSAGECOUNT, invalidInputMessageCount)
          PipeLineMetadataService.setLongMetricValue(MarketingConstants.CORRUPTROWS, invalidInputMessageCount)

          log.error("Saving corrupted records to : " + corruptRecordLocation)
          log.error("Saving corrupted records as : " + format)
          invalidDF.write.mode(MarketingConstants.APPEND).format(format).save(corruptRecordLocation)
          outputDF.drop(MarketingConstants._CORRUPT_RECORD)
        } else {
          log.info("No corrupted records detected!")
          PipeLineMetadataService.setLongMetricValue(MarketingConstants.INVALIDINPUTMESSAGECOUNT, 0)
          PipeLineMetadataService.setLongMetricValue(MarketingConstants.CORRUPTROWS, 0)

          inputDF.drop(MarketingConstants._CORRUPT_RECORD)
        }

      } else {
        log.info("No corrupted records detected!")
        PipeLineMetadataService.setLongMetricValue(MarketingConstants.INVALIDINPUTMESSAGECOUNT, 0)
        PipeLineMetadataService.setLongMetricValue(MarketingConstants.CORRUPTROWS, 0)

        inputDF
      }
    }
    validDF
  }

  private def initializeDataFrameReader(format: String, schema: StructType, header: Boolean,delimiter:String,
                                        options:Map[String,String])
                                       (implicit sparkSession: SparkSession): DataFrameReader = {
    val initDFR: DataFrameReader = sparkSession.read.option("encoding", "UTF-8").format(format)
    val schemaDFR: DataFrameReader = if (schema.nonEmpty) initDFR.schema(schema) else initDFR
    val headerDFR: DataFrameReader = if (header) schemaDFR.option("header","true") else schemaDFR
    val delimiterDFR: DataFrameReader = if (delimiter.nonEmpty) headerDFR.option("delimiter",delimiter) else headerDFR
    val optionsDFR: DataFrameReader = if (options.nonEmpty) delimiterDFR.options(options) else delimiterDFR

    optionsDFR
  }

  private def initializeSchema(schemaLocation: String, fileFormat: String, predeterminedSchema: StructType)
                              (implicit sparkSession: SparkSession): StructType = {
    if (schemaLocation.nonEmpty && predeterminedSchema.nonEmpty) {
      throw SchemaException("Unable to determine whether to use pre-determined schema or read schema from template. " +
        "Choose only one argument to pass schema, not both.")
    }

    if (schemaLocation.nonEmpty) {
      log.info("Using schema from provided template file : " + schemaLocation)
      sparkSession.read.format(fileFormat).load(schemaLocation).schema
        .add(MarketingConstants._CORRUPT_RECORD, "string")
    } else if (predeterminedSchema.nonEmpty) {
      log.info("Using pre-defined schema")
      predeterminedSchema
    } else {
      log.info("Reading file without applying a known schema")
      new StructType()
    }
  }

  /**
   * Reads input json files from kafka topic.The json given has "key" and "value" fields,
   * and the actual json is contained in the "value" field. The outsideSchema has the "key"/"value" json
   * and inputSchema is the actual message.
   *
   * @param readKafkaFiles Initialized ReadKafkaFiles object
   * @param sparkSession spark session
   * @return A DataFrame from ephemeral data, a List of the ephemeral partitions read, a Long corresponding to the final row count
   */
  def readJsonKafkaFile(readKafkaFiles: ReadKafkaFiles)(implicit sparkSession: SparkSession): (DataFrame, List[String],Long) = {
    val readDF = FileReader.readAndDedupeKafkaFiles(readKafkaFiles, false)
    (readDF._1,readDF._2,readDF._3)
  }


  /**
   * Reads from Kelsa DIP landed Kafka ephemeral partitions to a DataFrame. Supports deduplicating the Kafka records in
   * the DataFrame before returning it to the caller.
   *
   * @param readKafkaFiles An initialized ReadKafkaFiles object
   * @param dedupe Set to true to deduplicate the Kafka messages in the DataFrame; set false to not dedupe. Defaults to true.
   * @param testEphemeralLikeProd Set to true if in local testing the ephemeral location has partitioned subfolders that would
   *                              mimic the Kelsa DIP ephemeral, eg starting with "load_d=". Set to false if local testing the
   *                              ephemeral location just has data files without partitioned subfolders.
   * @param sparkSession A spark session
   * @return A DataFrame from ephemeral data, a List of the ephemeral partitions read, a Long corresponding to the final row count,
   *         and a String representing the Kelsa DPP checkpoint value in yyyyMMddhhss format (eg 202208130712). The checkpoint
   *         value is from the prior DPP run, indicating at which point in ephemeral partitions this current run started to
   *         read from. Use this value at the end of the current DPP run to update that same checkpoint value using the
   */
  def readAndDedupeKafkaFiles(readKafkaFiles: ReadKafkaFiles, dedupe: Boolean = true, testEphemeralLikeProd: Boolean = false)
                             (implicit sparkSession: SparkSession): (DataFrame, List[String],Long, String) = {


    val checkPointValue: String = CompactorService.getCheckPointValue(readKafkaFiles.checkpointPath)
    val listedValidPaths: List[String] =  if (readKafkaFiles.env != "local" || testEphemeralLikeProd) {

      val processedFiles: String = readKafkaFiles.ephemeralFilePathPrefix + checkPointValue
      log.info("Last processed File =" + processedFiles)
      log.info("NumberOfPartitions =" + readKafkaFiles.numberOfPartitions)
      log.info("inputEphemeralPath =" + readKafkaFiles.inputEphemeralPath)

      //List load date partition folders for processing
      val listedValidPaths: List[String] = getListedValidPaths(readKafkaFiles.inputEphemeralPath
        , readKafkaFiles.checkpointPath
        , readKafkaFiles.numberOfPartitions
        ,readKafkaFiles.ephemeralFilePathPrefix
      )

      listedValidPaths
    } else {

      List(readKafkaFiles.inputEphemeralPath)
    }
    log.info("Capture schema based on template file")
    val templateSchemaDF: DataFrame = sparkSession.read.format(readKafkaFiles.templateFileFormat).load(readKafkaFiles.templateLocation)
    val templateSchema: StructType = templateSchemaDF.schema

    log.info("Beginning reading of dataframe")
    val ephemeralDF: DataFrame = sparkSession.read.format(readKafkaFiles.inputEphemeralFileFormat)
      .schema(templateSchema).load(listedValidPaths: _*)

    log.info("HandleCorruptRecords")
    val ValidDF = handleCorruptRecords(ephemeralDF, readKafkaFiles.corruptRecordLocation,
      readKafkaFiles.inputEphemeralFileFormat)

    log.info("Reading value attribute to get the schema for value attribute which will be " +
      "read later after applying this schema")
    val ephemeralValueDF: DataFrame = sparkSession.read.json(templateSchemaDF.select("value").as[String](sparkSession.implicits
      .newStringEncoder))
    val ephemeralValueSchema: StructType = ephemeralValueDF.schema

    log.info("Extract value field and apply schema")
    val ephemeralValueSchemaAppliedDF: DataFrame = sparkSession.read.schema(ephemeralValueSchema)
      .json(ValidDF.select("value").as[String](sparkSession.implicits
        .newStringEncoder))

    val ephemeralValueSchemaAppliedDropColDF: DataFrame = if (readKafkaFiles.dropColumns.nonEmpty) {
      log.info("Dropping attributes=" + readKafkaFiles.dropColumns)
      ephemeralValueSchemaAppliedDF.drop(readKafkaFiles.dropColumns: _*)
    }
    else
    {
      ephemeralValueSchemaAppliedDF
    }

    log.info("Get new schema for the value field after dropping columns if any")
    val jsonSchema = ephemeralValueSchemaAppliedDropColDF.schema

    log.info(s"Apply new schema after dropping attributes if any and associate this to new attribute ${readKafkaFiles.kafkaValueFieldRename}" +
      " in the dataframe")
    val finalDF = if (!readKafkaFiles.addKafkaTimestamp) {
      ValidDF.withColumn(readKafkaFiles.kafkaValueFieldRename, from_json(col("value"), jsonSchema))
    } else {
      val intermediateDF = ValidDF
        .withColumn(readKafkaFiles.kafkaDateFieldName, to_date(col("timestamp")))
        .withColumn(readKafkaFiles.kafkaTimestampFieldName, to_timestamp(col("timestamp")))
        .withColumn(readKafkaFiles.kafkaValueFieldRename, from_json(col("value"), jsonSchema))
      intermediateDF.select(
        readKafkaFiles.kafkaDateFieldName, readKafkaFiles.kafkaTimestampFieldName,
        s"${readKafkaFiles.kafkaValueFieldRename}.*"
      )
    }

    log.info(s"Final output Schema after reading the input files.After flattening attributes ,prefix ${readKafkaFiles.kafkaValueFieldRename} " +
      "can be renamed using the method removeTempColumnPrefix in the common utils")
    finalDF.printSchema()

    val dedupeDF: DataFrame = if (dedupe) {
      MessageCleanser.removeDuplicates(finalDF)
    } else {
      finalDF
    }

    val outputDFCount = dedupeDF.count()
    PipeLineMetadataService.setLongMetricValue(DPPMetricsConstants.INPUTROWSCOUNT, outputDFCount)

    (dedupeDF,listedValidPaths,outputDFCount,checkPointValue)
  }

  /**
   * Used for reading from the Kelsa DIP ephemeral landed files. Finds all the ephemeral sub-folders which were not
   * read since the last DPP run.
   *
   * @param inputLocation The location of the ephemeral location to read files from. This should match the DIP's
   *                      inputEphemeralPath configuration {{{app.atomic.ephemeral.dirPath}}}. Refer to #4 on the
   *                      [[https://git.target.com/datasciences-de-kelsa/kelsa-dip#frequently-asked-questions-faq]]
   *                      for a full explanation of the handshake between the DIP and DPP in the Kelsa framework.
   * @param checkPointPath The location for the DPP to write its last read ephemeral partition to. This is in turn used
   *                       by the DIP as part of its handshake to delete ephemeral partitions earlier than this partition.
   *                       This value should match the DIP's deleteCheckPointPath configuration {{{app.atomic.ephemeral.deleteCheckPointPath}}}.
   *                       Refer to #4 on the [[https://git.target.com/datasciences-de-kelsa/kelsa-dip#frequently-asked-questions-faq]]
   *                       for a full explanation of the handshake between the DIP and DPP in the Kelsa framework.
   * @param inputFileCount The number of readers configured by the DIP when reading the Kafka topic. This should generally be equal
   *                       or less than the number of partitions in the Kafka topic itself. There will be one file written to each
   *                       ephemeral folder for each Kafka partition that has new offsets since the last DIP run. This value should
   *                       match the DIP's fileCount configuration {{{app.atomic.ephemeral.fileCount}}}.
   *                       Refer to #4 on the [[https://git.target.com/datasciences-de-kelsa/kelsa-dip#frequently-asked-questions-faq]]
   *                       for a full explanation of the handshake between the DIP and DPP in the Kelsa framework.
   * @param ephemeralFilePathPrefix Defaults to "load_date=", this is the prefix of the ephemeral directories which are written
   *                                by the DIP each run. Shouldn't have to ever specify a different value, except if there
   *                                is any kind of custom job which would rename ephemeral subfolders in between the DIP
   *                                and DPP runs. There presently is no way to alter the folder names through the DIP configurations.
   * @param sparkSession An initialized spark session
   * @return The list of paths under the ephemeral folder which are greater than the last checkpoint read. Should the last
   *         checkpoint read be empty, would return a list of all paths under the ephemeral folder.
   */
  def getListedValidPaths(inputLocation: String,
                          checkPointPath: String ,
                          inputFileCount : Int,
                          ephemeralFilePathPrefix:String= "load_date=")
                         (implicit sparkSession: SparkSession): List[String] = {

    log.info("Get the max partitioned date processed based on previous run")
    val processedFiles: String=ephemeralFilePathPrefix + CompactorService.getCheckPointValue(checkPointPath)(sparkSession)
    //processedFiles: String = load_date=202004230349

    log.info("Get list of valid ephemeral paths based on last processed files")
    val listedValidPaths : List[String] = CompactorService.listValidFilePathsGreaterThanPartition(inputLocation,inputFileCount,processedFiles)(sparkSession)
    // val listedValidPaths : List[String] =
    // List("hdfs://bigred3ns/common/SVSFFEHDP/data/str-tasks-events/data_ingestion_pipeline/ephemeral/load_date=202009080327")

    log.info("listedValidPaths=" + listedValidPaths.mkString(","))
    log.info("listedValidPathsSize=" + listedValidPaths.length)

    listedValidPaths
  }

}

