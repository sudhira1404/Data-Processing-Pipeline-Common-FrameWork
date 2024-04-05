package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dsc.kelsa.datapipeline.core.util.KelsaUtil
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.service.FileReader.readFileToDataFrame
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.{getFileNameAndExtensionFromFileName, getFilePathFileNameExtensionFromFilePath}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Collections, Date}
import scala.sys.process._

object HdfsUtils extends LoggingTrait {

  /**
   * Gets hadoop file system
   *
   * @param sqlContext : SQLContext
   * @return hadoop file system
   */

  def getHadoopFileSystem(sqlContext: SQLContext): FileSystem = {

    val config = sqlContext.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config)

    fs
  }

  def getFileSystemAndCurrentWorkingPath(path: String)(implicit spark: SparkSession): (FileSystem, String) = {
    val fs = getHadoopFileSystem(spark.sqlContext)
    val cwd: Path = fs.getWorkingDirectory
    val fullOutPath = MarketingCommonUtils.appendSlashInEnd(new Path(cwd, path).toString)
    (fs, fullOutPath)
  }
  /**
   * Gets list of files from the hdfsFileLocation along with file size in tuple
   *
   * @param hdfsFileLocation : hdfsFileLocation.Can be directory or directory with filename
   * @return list of tuple (hdfs filename,hdfs filesize)
   */

  def getListOfHdfsFiles(hdfsFileLocation: String)(implicit sparkSession: SparkSession): List[(String, Long)] = {

    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    var listOfSftpFiles: List[(String, Long)] = List()
    val fileStatus = fs.listStatus(new org.apache.hadoop.fs.Path(hdfsFileLocation))
    val fileList = fileStatus.map(x => x.getPath.toString)
    fileList.foreach(file => {
      val fileSize = getHdfsFileSize(file)
      listOfSftpFiles = listOfSftpFiles :+ (file, fileSize)
    }
    )

    listOfSftpFiles

  }

  /**
   * Gets file size from the file in the hdfsFileLocation
   *
   * @param hdfsFileLocation : hdfsFileLocation.hdfs Directory with filename
   * @return file size
   */

  def getHdfsFileSize(hdfsFileLocation: String)(implicit sparkSession: SparkSession): Long = {

    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    val hdfsFileLocationPath = new org.apache.hadoop.fs.Path(hdfsFileLocation)
    val hdfsFileContentSummary = fs.getContentSummary(hdfsFileLocationPath)
    hdfsFileContentSummary.getLength

  }

  /**
   * Delete hdfs file at the hdfs location.
   *
   * @param hdfsLocation      :String hdfsDirectory or hdfsDirectory with filename
   * @param fileSearchPattern :String Defaulted to *.Searches based on the file pattern in hdfsLocation and deletes
   * @param recursive         :Boolean.Defaulted to false.If set will delete recursively
   * @return unit
   */

  def hdfsDirDelete(hdfsLocation: String, fileSearchPattern: String = "*", recursive: Boolean = false)
                   (implicit sparkSession: SparkSession): Unit = {

    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    val hdfsLocationPath = new org.apache.hadoop.fs.Path(hdfsLocation)
    if (fs.exists(hdfsLocationPath)) {
      val deletedHdfsPaths: Array[Path] = if (fs.isFile(hdfsLocationPath)) {
        Array(hdfsLocationPath)
      }
      else {
        fs.globStatus(new Path(s"$hdfsLocation/$fileSearchPattern")).map(_.getPath)
      }
      deletedHdfsPaths.foreach { path => fs.delete(path, recursive); log.info(s"File ${path.toString} is deleted") }
    }
    else {
      log.info(s"Hdfs path $hdfsLocation does not exists")
    }

  }

  /**
   * Copy local file into HDFS
   *
   * @param localFileLocation : local File location with filename
   * @param hdfsFileLocation  : HDFS location with filename
   * @return
   */
  def localFileCopyToHdfs(localFileLocation: String,
                          hdfsFileLocation: String)(implicit sparkSession: SparkSession): Unit = {

    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    val hdfsPath = new org.apache.hadoop.fs.Path(hdfsFileLocation)
    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(localFileLocation), new org.apache.hadoop.fs.Path(hdfsFileLocation))
    log.info(s"File copied to hdfs=>$hdfsFileLocation/${hdfsPath.getName} from ${localFileLocation}")

  }

  /**
   * Copy file from hdfs to local
   *
   * @param hdfsFileLocation  : hdfsFileLocation with filename
   * @param localFileLocation : localFileLocation with filename
   * @return : Unit
   */
  def localFileCopyFromHdfs(hdfsFileLocation: String,
                            localFileLocation: String)(implicit sparkSession: SparkSession): Unit = {
    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    fs.copyToLocalFile(new org.apache.hadoop.fs.Path(hdfsFileLocation), new org.apache.hadoop.fs.Path(localFileLocation))
    log.info(s"File copied to local ${localFileLocation} from hdfs=>$hdfsFileLocation")

  }


  /** *
   * List hdfs files and hdfs file paths based on hdfsFileSearchPattern look up if passed
   * and ignores files starting with underscore.It recursively looks for files if directory is present
   * in the hdfsFileLocation
   *
   * @param hdfsFileLocation      : Hdfs file location
   * @param hdfsFileSearchPattern : Defaulted to empty string.Will look for hdfs file based on this pattern
   * @return List of qualified hdfs files and hdfs full file path as tuple
   */

  def listOfQualifiedHdfsFiles(
                                hdfsFileLocation: String,
                                hdfsFileSearchPattern: String = ""
                              )(implicit sparkSession: SparkSession): (List[String], List[String]) = {


    var listOfQualifiedHdfsFileLocations: List[String] = List()
    var listOfQualifiedHdfsFileNames: List[String] = List()
    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    val listOfHdfsFiles = fs.listStatus(new org.apache.hadoop.fs.Path(hdfsFileLocation))
    for (files <- listOfHdfsFiles) {
      if (fs.getFileStatus(files.getPath).isDirectory) {
        val hdfsDirectoryPath = files.getPath.toString.substring(files.getPath.toString.lastIndexOf("/") + 1)
        log.info(s"Hdfs directory detected $hdfsDirectoryPath,will recursively fetch the files")
        listOfQualifiedHdfsFiles(files.getPath.toString, hdfsFileSearchPattern)
      } else {
        val hdfsFileLocation = files.getPath.toString
        val hdfsFileName = hdfsFileLocation.substring(hdfsFileLocation.lastIndexOf("/") + 1)
        if (hdfsFileSearchPattern == "" && !hdfsFileName.startsWith("_") || (hdfsFileSearchPattern != "" && hdfsFileName.contains(hdfsFileSearchPattern))) {
          listOfQualifiedHdfsFileLocations = listOfQualifiedHdfsFileLocations :+ hdfsFileLocation
          listOfQualifiedHdfsFileNames = listOfQualifiedHdfsFileNames :+ hdfsFileName
        }
      }
    }
    (listOfQualifiedHdfsFileNames, listOfQualifiedHdfsFileLocations)
  }


  /** *
   * Renames file created by spark(like part-*) with renameFileName parameter.
   * If set ,file in hdfsSourceFilePath will be renamed to renameFileName in the hdfs
   * If there are multiple files in hdfsSourceFilePath which needs to be renamed while moving to hdfs
   * format of rename will be filename_$i.fileextension else will use the filename in  hdfsSourceFilePath
   *
   * @param hdfsSourceFilePath : Hdfs file location which needs to be renamed
   * @param hdfsDestFilePath   :  Hdfs file location which stores the renamed files
   * @param renameFileName     :  renamed file name
   * @param sparkSession       :  spark session
   * @return unit
   */

  def hdfsFileRename(hdfsSourceFilePath: String,
                     hdfsDestFilePath: String,
                     renameFileName: String,
                     hdfsFileSearchPattern: String = "",
                     deleteFileSearchPattern: String = "*",
                     deleteFileRecursive: Boolean = false)(implicit sparkSession: SparkSession): List[String] = {
    var hdfsSourceFileList: List[String] = List()
    try {
      val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
      var i = 1
      //do you need to empty the directory
      //if (hdfsDestFilePath.charAt(hdfsDestFilePath.length() - 1) != "/") hdfsDestFilePath + "/" else hdfsDestFilePath
      hdfsDirDelete(hdfsDestFilePath, deleteFileSearchPattern, deleteFileRecursive)
      hdfsSourceFileList = listOfQualifiedHdfsFiles(hdfsSourceFilePath, hdfsFileSearchPattern)._2
      log.info("list of hdfs files that will be renamed=>" + hdfsSourceFileList)
      if (hdfsSourceFileList.length >= 1) {
        hdfsSourceFileList.foreach { paths =>
          log.info(s"File to be renamed $paths")
          val renameFileNameNoExt = getFileNameAndExtensionFromFileName(renameFileName)._1
          val renameFileNameExt = getFileNameAndExtensionFromFileName(renameFileName)._2
          val renamedHdfsSourceFileName = hdfsSourceFileList.length match {
            case x if x > 1 =>
              s"${renameFileNameNoExt}_$i.${renameFileNameExt}"
            case _ => renameFileName
          }
          //hdfsDirDelete(hdfsDestFilePath + "/" + renamedHdfsSourceFileName)
          fs.rename(new Path(paths), new Path(hdfsDestFilePath + "/" + renamedHdfsSourceFileName))
          log.info(s"Renamed the file in the directory ${hdfsDestFilePath + "/" + renamedHdfsSourceFileName} from ${paths} ")
          i = i + 1
        }
      }
    }
    catch {
      case e: Exception =>
        log.info("Error while renaming processing directory" + e)
    }
    hdfsSourceFileList
  }

  /**
   * Will save DF to hdfs location
   *
   * @param inputDF                     DF to be stored in hdfs
   * @param selectColumns               Defaulted to empty.selectColumns to be fetched from inputDF
   * @param renameColumns               Defaulted to empty.renameColumns if any
   * @param writeMode                   Defaulted to overwrite to be used while saving DF to HDFS
   * @param fileFormat                  Defaulted to orc to be used while saving DF to HDFS
   * @param fileSaveLocation            Hdfs save file location
   * @param coalesce                    Defaulted to  1 to be used while saving DF to HDFS
   * @param options                     Options to be used while saving DF to HDFS.like
   *                                    Map("header"->"true","sep"->",","mapreduce.fileoutputcommitter.marksuccessfuljobs"->"false")
   * @param hdfsSaveFlag                Defaulted to true.If set will save result in hdfs else will ignore saving result.
   * @return inputDF
   */

  def hdfsSaveFromDF(inputDF: DataFrame,
                     selectColumns: Seq[String] = Seq.empty[String],
                     renameColumns: Seq[String] = Seq.empty[String],
                     writeMode:String="overwrite",
                     fileFormat:String="orc",
                     fileSaveLocation:String,
                     coalesce:Int=0,
                     options:Map[String,String] = Map.empty,
                     hdfsSaveFlag:Boolean=true
                    ): DataFrame = {

    if (hdfsSaveFlag) {
      val inputSelectColsDF = if (selectColumns.nonEmpty) inputDF.select(selectColumns.map(name => col(name)): _*) else inputDF
      val inputRenameColsDF = if (renameColumns.nonEmpty) inputSelectColsDF.toDF(renameColumns: _*) else inputSelectColsDF

      inputRenameColsDF
        .coalesce(coalesce)
        .write
        .mode(writeMode)
        .options(options)
        .format(fileFormat)
        .save(fileSaveLocation)
    }
    inputDF
  }

  /**
   * Will copy files from one directory to another,one file at a time
   *
   * @param listOfFilteredHdfsFileLocations    list of directories or files to be copied
   * @param inputEphemeralPath                 Target destination
   * @param isHdfsFileSystem                   If true the filepath is in hdfs else local

   * @return date in the file in the path hdfsStartDateFilePath
   */

  def hdfsCopy(listOfFilteredHdfsFileLocations:List[String],
               inputEphemeralPath:String,
               isHdfsFileSystem: Boolean)(implicit sparkSession: SparkSession): Unit = {


    for (hdfsFileLocations <- listOfFilteredHdfsFileLocations) {
      log.info(s"Will copy  file from $hdfsFileLocations to $inputEphemeralPath ")

      if (isHdfsFileSystem) {
        s"hdfs dfs -cp $hdfsFileLocations $inputEphemeralPath".!!
      }
      else {
        s"cp -r ${hdfsFileLocations.split(":").last} $inputEphemeralPath".!!
      }
    }
  }

  /**
   * Gets date present in the file
   *
   * @param hdfsStartDateFormat    Format of the date in the file in location hdfsStartDateFilePath
   * @param hdfsStartDateFilePath  File path
   * @param isHdfsFileSystem       If true the filepath is in hdfs else local

   * @return date in the file in the path hdfsStartDateFilePath
   */


  def extractDateFromFileInHdfsLocation(hdfsStartDateFormat: String,
                                        hdfsStartDateFilePath: String,
                                        isHdfsFileSystem: Boolean)(implicit sparkSession: SparkSession): LocalDate = {

    val hdfsStartDateFilePathWithFilename = listOfQualifiedHdfsFiles(hdfsStartDateFilePath)._1.mkString(",")
    log.info(s"hdfsStartDateFilePathWithFilename=>$hdfsStartDateFilePathWithFilename")
    val hdfsStartDate = if (isHdfsFileSystem) {
      s"hdfs dfs -cat ${hdfsStartDateFilePath}/$hdfsStartDateFilePathWithFilename".!!
    }
    else {
      s"cat ${hdfsStartDateFilePath}/${hdfsStartDateFilePathWithFilename}".!!
    }
    val formatter = DateTimeFormatter.ofPattern(hdfsStartDateFormat)
    val startDate = LocalDate.parse(hdfsStartDate.trim, formatter)

    startDate
  }

  /**
   * Gets max date from list of files in the hdfs path.Date should be part of filename.
   *
   * @param listOfQualifiedHdfsFileLocations             List of files with date in the filename
   * @param dateFormat                                   Defaulted to "yyyy-MM-dd".Date format of the output.
   * @param readDateFromFileElseArchive                  Defaulted to true.If set then will read file from hdfsLandingFileLocation with date in the filename
   *                                                     else will read from the archive location with folder being in the format date=2023-01-01.
   * @param patternSearchReadDateFromFileElseArchive     Defaulted to yyyy-MM-dd.WIll search filename in hdfsLandingFileLocation based on this or
   *                                                     in archive location date=date folder will be searched based on this

   * @return max date from the list of files
   */

   def extractMaxDateFromListOfHdfsFilePathsContainingDate(listOfQualifiedHdfsFileLocations: List[String],
                                                                  dateFormat:String="yyyy-MM-dd",
                                                                  readDateFromFileElseArchive:Boolean=true,
                                                                  patternSearchReadDateFromFileElseArchive:String="\\d{4}-\\d{2}-\\d{2}"): LocalDate = {

    val datesOfQualifiedHdfsFileLocations = if (readDateFromFileElseArchive) {
      val pattern = patternSearchReadDateFromFileElseArchive.r
      listOfQualifiedHdfsFileLocations.map(path => pattern.findAllIn(path.split("/").last).mkString(",")).mkString(",")
    } else
    {
      listOfQualifiedHdfsFileLocations.map(path => path.split("=").last).mkString(",")
    }
    //val dateFormat = "yyyy-MM-dd"
    val formatter = new SimpleDateFormat(dateFormat)
    val distinctDatesList = new util.ArrayList[Date]
    datesOfQualifiedHdfsFileLocations.split(",").map(a => distinctDatesList.add(formatter.parse(a)))
    val maxDateString = formatter.format(Collections.max(distinctDatesList))
    val dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
    val maxDateOfQualifiedHdfsFileLocations = LocalDate.parse(maxDateString, dateTimeFormatter)
    log.info(s"Max Date from the list of HdfsFileLocation=>${listOfQualifiedHdfsFileLocations} =$maxDateOfQualifiedHdfsFileLocations")
    maxDateOfQualifiedHdfsFileLocations
  }

  /**
   * Gets list of files from the hdfsFileLocation
   *
   * @param hdfsFileLocation : hdfsFileLocation.Can be directory or directory with filename
   * @return list of hdfs file location along with filename
   */

  def listOfHdfsFiles(hdfsFileLocation: String)(implicit sparkSession: SparkSession): List[String] = {

    var listOfQualifiedHdfsFileLocations: List[String] = List()
    val fs = KelsaUtil.hadoopConfigurations(sparkSession)
    val listOfHdfsFiles = fs.listStatus(new org.apache.hadoop.fs.Path(hdfsFileLocation))
    for (files <- listOfHdfsFiles) {
      if (fs.getFileStatus(files.getPath).isDirectory) {
        val hdfsFileLocation = files.getPath.toString
        listOfQualifiedHdfsFileLocations = listOfQualifiedHdfsFileLocations :+ hdfsFileLocation
      }

      else {
        val hdfsFileLocation = files.getPath.toString
        val hdfsFileName = hdfsFileLocation.substring(hdfsFileLocation.lastIndexOf("/") + 1)
        if (!hdfsFileName.startsWith("_")) {
          listOfQualifiedHdfsFileLocations = listOfQualifiedHdfsFileLocations :+ hdfsFileLocation
        }
      }
    }
    listOfQualifiedHdfsFileLocations
  }


  /**
   * Read comma separated file from hdfs location and appends new value to and moves to the hdfs location
   *
   * @param hdfsFileLocationWithFileName :  hdfs file name with path which will be appended with valueToBeAppended
   * @param valueToBeAppended      : comma separated string values to be appended to hdfsFileLocationWithFileName
   * @return Unit
   */


  def appendToHDFSFile(hdfsFileLocationWithFileName: String, valueToBeAppended: String)(implicit sparkSession: SparkSession): Unit = {

    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    var listOfRemoteFilesWithNoPath = List[String]()
    val listOfRemoteFiles=  valueToBeAppended.split(",").map(_.trim).toList
    for (x <- listOfRemoteFiles) {
      val filename = getFilePathFileNameExtensionFromFilePath(x)
      listOfRemoteFilesWithNoPath = listOfRemoteFilesWithNoPath :+ filename._2
    }

    val valueToBeAppendedNew = listOfRemoteFilesWithNoPath.mkString(",")

    log.info("Value to be appended to hdfs file which keeps track of sftp files====>" + valueToBeAppendedNew)

    val commaSepValFromHdfsFileLocation = readFileToDataFrame(
      fileLocation = hdfsFileLocationWithFileName,
      fileFormat = "csv").collect().mkString(",")

    log.info("Value read from hdfs file which keeps track of sftp files ===>" + commaSepValFromHdfsFileLocation)
    val commaSepValFromHdfsFileLocationTransformed = commaSepValFromHdfsFileLocation.replaceAll("\\[","").replaceAll("]","") + "," + valueToBeAppendedNew
    val os = fs.create(new Path(hdfsFileLocationWithFileName))
    try {
      os.write(commaSepValFromHdfsFileLocationTransformed.getBytes())
    } finally {
      os.close()
    }
    os.close()
  }


}
