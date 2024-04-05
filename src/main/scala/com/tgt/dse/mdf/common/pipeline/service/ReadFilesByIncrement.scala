package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.util.DateTimeUtils.getDaysBetweenStartAndEndDate
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.moveEphemeralFilesToProcess
import com.tgt.dse.mdf.common.pipeline.util.HdfsUtils._
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.rounding
import com.tgt.dse.mdf.common.pipeline.{LoggingTrait, MarketingPipelineConfigs}
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter


object ReadFilesByIncrement extends  LoggingTrait {

  /**
   * Will read files from hdfsLandingFileLocation in increments and each time copies files to processing folder and calls
   * the main function or executePipeLine of type MarketingPipelineConfigs till the maxEndDate is reached or till all the files are processed.
   * Date can be in the filename or it can look at archive partition location with structure date=2023-01-01
   *
   * Reads start date from hdfsStartDateFilePath and max end date from parameter maxEndDate if provided else max end in the
   * filename available in hdfsLandingFileLocation and calculate the loopCounter.Like if start is 2022-12-31
   * and max end date is 2023-01-10 or max date in the filename is 2023-01-10 and if increment is 2 then
   * loopCounter will be daysbetween 2022-12-31  and 2023-01-10=10 divided by increment=2 equaling to 5 times.
   * If decimal will round to next.Loop will run 5 times and each time will fetch 2 files starting from
   * 2023-01-01 till 2023-01-02 and moved to processing and calls the executePipeline and once done will update
   * next startDate in hdfsStartDateFilePath as 2023-01-02 and endDate as 2023-01-04 and fetch files greater than
   * 2023-01-02 and less than equal 2023-01-04 and less than max end date 2023-01-10.Keeps continuing till
   * all file with date 2023-01-10 is processed.If in any loop if there are no files fetched then will increment the loop.
   *
   * @param  f                                           MarketingPipelineConfigs => Unit,Takes a function with MarketingPipelineConfigs as input and unit as output like PipeLineService.executePipeLine
   * @param config                                       Config object of type MarketingPipelineConfigs
   * @param hdfsLandingFileLocation                      Input file location to be processed in increments
   * @param increment                                    Defaulted to 0.If specified will read files from hdfsLandingFileLocation by increment.
   *                                                     else will fetch one file at a time
   * @param inputDateFormat                              Date format in the filename and in hdfsStartDateFilePath
   * @param hdfsStartDateFilePath                        StartDate to be used to fetch files(will parse the date in the filename).Will fetch files greater than this date
   * @param maxEndDate                                   Will fetch files till this date if specified else will get max date avalaible in the fielname in the path hdfsLandingFileLocation and fetched till this date
   * @param isHdfsFileSystem                             Defaulted to  false.If true will fetch hdfsStartDateFilePath from hdfs else local
   * @param readDateFromFileElseArchive                  Defaulted to true.If set then will read file from hdfsLandingFileLocation with date in the filename used to fetch in incremental
   *                                                     else will read from the archive location with folder being in the format date2023-01-01.
   * @param patternSearchReadDateFromFileElseArchive     Defaulted to yyyy-MM-dd.WIll search filename in hdfsLandingFileLocation based on this or
   *                                                     in archive location date=date folder will be searched based on this
   * @param deleteSource                                 Defaulted to false.If set, will delete files from srcpath.
   * @return list of hdfs files processed
   *         This could be called like below in the DPP main
   *         def executeHistoryPipeLine(config: MarketingPipelineConfigs): Unit = {
   *
   *         val pipeLineService = PipeLineHelper(config.pipeLineConfig)
   *         implicit val sparkSession = config.sparkSession
   *
   *         readFilesByIncrement(PipeLineService.executePipeLine,
   *         config: MarketingPipelineConfigs,
   *         pipeLineService.historyHdfsLandingFileLocation,
   *         pipeLineService.inputEphemeralPath,
   *         increment=3,
   *         "yyyy-MM-dd",
   *         pipeLineService.historyHdfsStartDateFilePath,
   *         maxEndDate = pipeLineService.historyMaxEndDate,
   *         readDateFromFileElseArchive=false)
   *
   *         }
   *
   *         executeCommonMainPipeline(args: Array[String],executeHistoryPipeLine)
   *
   */
  //noinspection ScalaStyle

  def readFilesByIncrement (
                             f: MarketingPipelineConfigs => Unit,
                             config: MarketingPipelineConfigs,
                             hdfsLandingFileLocation:String,
                             inputEphemeralPath:String,
                             increment:Int=0,
                             inputDateFormat:String,
                             hdfsStartDateFilePath:String,
                             maxEndDate:String="",
                             isHdfsFileSystem:Boolean=true,
                             readDateFromFileElseArchive:Boolean=true,
                             patternSearchReadDateFromFileElseArchive:String="\\d{4}-\\d{2}-\\d{2}",
                             deleteSource:Boolean=false
                           )(implicit sparkSession: SparkSession):List[String] ={

    import sparkSession.implicits._

    var listOfHdfsFileLocationsProcessed: List[String] = List()
    val env  = config.pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    val listOfQualifiedHdfsFileLocations= listOfHdfsFiles(hdfsLandingFileLocation)
    log.info(s"list of files in the HdfsFileLocation $hdfsLandingFileLocation =>${listOfQualifiedHdfsFileLocations.mkString(",")}")
    val hdfsTotalFileLocationCount = listOfQualifiedHdfsFileLocations.size
    log.info(s"Total count of files in the HdfsFileLocation=>${hdfsTotalFileLocationCount}")
    val startDateInTheHdfsFileLocation = extractDateFromFileInHdfsLocation(inputDateFormat, hdfsStartDateFilePath, isHdfsFileSystem)
    var completedStartDate =startDateInTheHdfsFileLocation
    val maxDateOfQualifiedHdfsFileLocations = if (maxEndDate != "") {
      LocalDate.parse(maxEndDate, DateTimeFormatter.ofPattern(inputDateFormat) )
    }  else {
      extractMaxDateFromListOfHdfsFilePathsContainingDate(listOfQualifiedHdfsFileLocations,inputDateFormat,readDateFromFileElseArchive)
    }
    val daysBetween = getDaysBetweenStartAndEndDate(startDateInTheHdfsFileLocation,maxDateOfQualifiedHdfsFileLocations)
    log.info(s"Total days between start date $startDateInTheHdfsFileLocation and max end date $maxDateOfQualifiedHdfsFileLocations of files in HdfsFileLocation=>${hdfsLandingFileLocation}=$daysBetween")
    val incrementNew = if (increment ==0) daysBetween else increment
    val loopCounter =rounding(daysBetween / incrementNew ,0.0)
    log.info(s"Number of times loop will be executed is ${loopCounter} times and " +
      s"each time $increment files will be picked starting from date greater than in path $hdfsStartDateFilePath and end date being incremented by $increment." +
      "Between these date interval files will be picked ")
    for( a <- 1 until loopCounter+1) {
      log.info(s"Inside loop with counter=$a")
      val startDate = completedStartDate
      val endDateFormatter = DateTimeFormatter.ofPattern(inputDateFormat)
      val endDate = startDate.plusDays(incrementNew).format(endDateFormatter)
      log.info(s"startDate=$startDate for counter=$a")
      log.info(s"endDate=$endDate for counter=$a")
      log.info(s"maxEndDate=$maxDateOfQualifiedHdfsFileLocations for counter=$a")
      val listOfFilteredHdfsFileLocations = if (readDateFromFileElseArchive){
        val pattern = patternSearchReadDateFromFileElseArchive.r
        listOfQualifiedHdfsFileLocations.filter(path => pattern.findAllIn(path.split("/").last).mkString(",") > startDate.toString &&  pattern.findAllIn(path.split("/").last).mkString(",") <= endDate &&  pattern.findAllIn(path.split("/").last).mkString(",") <= maxDateOfQualifiedHdfsFileLocations.toString)
      } else {
        listOfQualifiedHdfsFileLocations.filter(path => path.split("=").last > startDate.toString &&  path.split("=").last <= endDate &&  path.split("=").last <= maxDateOfQualifiedHdfsFileLocations.toString)
      }
      if (!listOfFilteredHdfsFileLocations.isEmpty) {
        log.info(s"list of files between start and end date $startDate and $endDate with maxEndDate being $maxDateOfQualifiedHdfsFileLocations=$listOfFilteredHdfsFileLocations for counter=$a")
        hdfsDirDelete(inputEphemeralPath,recursive=true)
        if (readDateFromFileElseArchive) {
          moveEphemeralFilesToProcess(listOfFilteredHdfsFileLocations, inputEphemeralPath, deleteSource)
        } else {
          hdfsCopy(listOfFilteredHdfsFileLocations, inputEphemeralPath, isHdfsFileSystem)
        }
        if (env != "local"){
          log.info("Execute the function")
          f(config)
        } else {
          log.info("Skipping executing the function")
        }
        listOfHdfsFileLocationsProcessed = listOfHdfsFileLocationsProcessed :+ listOfFilteredHdfsFileLocations.mkString(",")
      } else {
        log.info(s"list of files between start and end date $startDate and $endDate with maxEndDate being $maxEndDate is empty for counter=$a")
        //do you want to update checkpoint
      }
      log.info(s"Update end date $endDate in the path $hdfsStartDateFilePath")
      completedStartDate = LocalDate.parse(endDate, DateTimeFormatter.ofPattern(inputDateFormat))
      val hdfsDateDF=Seq(endDate).toDF("date")
      hdfsSaveFromDF(hdfsDateDF,coalesce=1,fileFormat="csv",fileSaveLocation=hdfsStartDateFilePath,options=Map("header"->"false"))
    }

    log.info(s"list of files processed for dates > $startDateInTheHdfsFileLocation and  <= $maxDateOfQualifiedHdfsFileLocations=>$listOfHdfsFileLocationsProcessed")
    listOfHdfsFileLocationsProcessed
  }


}
