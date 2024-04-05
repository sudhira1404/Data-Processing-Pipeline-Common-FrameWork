package com.tgt.dse.mdf.common.pipeline.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.tgt.dsc.kelsa.datapipeline.core.service.CompactorService
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.FileMoveException
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.withRetryAndThrowException
import com.tgt.dse.mdf.common.pipeline.util.{DateTimeUtils, HdfsUtils}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.json4s.jackson.JsonMethods

import java.io.{BufferedOutputStream, File, IOException}
import java.util
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
 * @param srcpath              source path from where files are moved to destpath.
 * @param destpath                          : target path where file will be moved.
 * @param zerobyteabort                :if set to true,will check for zerobyte file in the srcpath.
 * @param dbName               defaulted to empty string.If set,will be used to create partition on the hive table after file are moved from src to dest.
 * @param partition            defaulted to false.If set,will create partition on the hive table after file are moved from src to dest.
 *                             Partitions are derived based on file date or current date.
 * @param tablename            defaulted to empty string.If set,will be used to create partition on the hive table after file are moved from src to dest.
 * @param droppartition        defaulted to false.If set,will drop partition on the hive table after file are moved from src to dest.
 *                             Partitions are derived based on file date or current date.
 * @param deletesource         defaulted to true.If set, will delete files from srcpath.
 * @param overwrite            defaulted to true.If set,will overwrite files in destpath else will delete in destpath and copy.
 * @param dynamicJsonToCsv     defaulted to false.If set,will parse the dynamicJson file and move to destpath
 *                             so that it could be easly accessible through a hive table.
 * @param jdbcUrl              defaulted to empty string.If set,will be used to create partition on the hive table after file are moved from src to dest.
 * @param queueName            defaulted to empty string.If set then will be used to create partition on the hive table after file are moved from src to dest.
 * @param filter               defaulted to empty string.Used to apply file filter when fetching from srcpath.
 * @param fileDateRegexPattern defaulted to empty string.If not set and param fileDate is true,
 *                             will work for media stats files which looks for report-yyyy-mm-dd format.
 *                             If not set and param fileDate is false,will appends current date along with partitionColName to destpath to get partition path.
 *                             If set,will be used to look up pattern in the filename to extract the date and
 *                             appends the date along with partitionColName to destpath to get partition path.
 *                             This should be used along with other parameters that are used create partition
 *                             on the hive table after file are moved from src to dest
 * @param fileDate             defaulted to true indicating filename contains date.
 *                             If not set,will work for files which does not have date in the filename.
 *                             It considers current date for deriving partition date.
 * @param fileDateFormatter    defaulted to empty string.If not set will work for media stats files which looks for report-yyyy-mm-dd format
 *                             If set that apply the java date formatter to incoming date extracted from the file and converts to YYYY-MM-DD.
 *                             This should be used along with other parameters that are used create partition on the hive table
 *                             after file are moved from src to dest
 * @param partitionColName     defaulted to report_d string.If not set,will work for media stats files which looks for report-yyyy-mm-dd
 *                             format and create partition based on report_d.
 *                             If set then it creates partition based on this.
 */
final case class FileWriterService(srcpath: String,
                      destpath: String,
                      zerobyteabort: Boolean,
                      dbName: String = "",
                      partition: Boolean = false,
                      tablename: String = "",
                      droppartition: Boolean = false,
                      deletesource: Boolean = true,
                      overwrite: Boolean = true,
                      dynamicJsonToCsv: Boolean = false,
                      jdbcUrl: String = "",
                      queueName: String = "",
                      filter: String = "" ,
                      fileDateRegexPattern:String="",
                      fileDate:Boolean=true,
                      fileDateFormatter:String="",
                      partitionColName:String="report_d") {

}
object FileWriterService extends LoggingTrait {

  def hdfsMove(fileWriterService: FileWriterService)(implicit sparkSession: SparkSession): BigInt = {
    val fs = getFileSystem()
    val srcstatus: Array[FileStatus] = getFileStatusArray(fileWriterService.srcpath, fileWriterService.filter)
    val deststatus: Array[FileStatus] = getFileStatusArray(fileWriterService.destpath, "")
    var totFilesize: BigInt = 0

    if (srcstatus.length > 0) {
      srcstatus.foreach(x => {
        totFilesize += moveFile(x, fileWriterService, fs)
      }
      )
    }
    else {
      log.info(s"No source files found in the path ${fileWriterService.srcpath}")
      if (!fileWriterService.dynamicJsonToCsv) {
        log.info(s"Checking if destpath ${fileWriterService.destpath} has any file which is not zero byte file")
        if (deststatus.length > 0) {
          deststatus.foreach(x => {
            log.info("Size of dest file which was not moved in the prev run is: " + x.getLen)
            totFilesize += x.getLen
          })
        }
      } else {
        log.info(s"There are no files in the destpath ${fileWriterService.destpath} to be moved")
      }
    }
    if (fileWriterService.zerobyteabort) {
      if (totFilesize > 0) {
        log.info(s"file size is greater than zero bytes in the destpath ${fileWriterService.destpath} DPP process will start")
        totFilesize
      } else {
        log.info(s"file size is zero byte in the destpath path ${fileWriterService.destpath} DPP process will not start")
        //PipeLineMetadataService.setLongMetricValue(MarketingConstants.INPUTROWCOUNT, inputRowCount)
        throw FileMoveException(s"file size is zero byte in the destpath path ${fileWriterService.destpath}.DPP process will not start")
      }
    } else {
      totFilesize
    }

  }

  def moveFile(fileStatus: FileStatus, fileService: FileWriterService, fileSystem: FileSystem)
              (implicit sparkSession: SparkSession): BigInt = {
    var listOfPartitionDate = List.empty[String]
    var destpathpartition = fileService.destpath
    val filePath = fileStatus.getPath
    log.info("Source files to be moved are: " + filePath)
    log.info("Size of source files to be moved is: " + fileStatus.getLen)
    val fileName = new File(filePath.toString).getName
    if (fileName.endsWith("tar.gz")) {
      processTargz(fileStatus.getPath.toString, fileService.destpath)
      val fsAfterUntar = fileSystem.listStatus(new Path(fileService.destpath))
      val totalUntarredFiles = fsAfterUntar.length
      val untarredFileArray = new Array[String](totalUntarredFiles)
      var i = 0
      var totFileSize: BigInt = 0
      fsAfterUntar.foreach(f => {
        totFileSize += f.getLen
        untarredFileArray(i) = f.getPath.toString
        i += 1
      })
      log.info("Total file size in destination after untar: {}", totFileSize)
      log.info(s"Files untarred to dest path $destpathpartition are: ", untarredFileArray.mkString(","))

      if (fileService.deletesource) {
        fileSystem.delete(filePath, true)
        log.info("Deleted Source file: ", fileName)
      }
      totFileSize
    }
    else if (!fileName.startsWith(".")) {
      val totFilesize = fileStatus.getLen
      if (fileService.partition) {
        val findPartitionDate = DateTimeUtils.fileDateExtraction(fileName, fileService.fileDateRegexPattern, fileService.fileDate,
          fileService.fileDateFormatter, fileService.partitionColName)
        val partitionfolder = findPartitionDate._1
        val partitiondate = findPartitionDate._2
        destpathpartition = s"${fileService.destpath}$partitionfolder"
        if (listOfPartitionDate.contains(partitiondate)) {
          log.info(s"partition is already created for the partition date $partitiondate in the table ${fileService.tablename} in the current run" +
            ",will skip creating partition")
        } else {
          log.info(s"new partition will be created for the partition date $partitiondate in the table ${fileService.tablename}")
          listOfPartitionDate = listOfPartitionDate :+ partitiondate
          log.info("New dest path where files will be moved  : " + destpathpartition)
          val jdbcconn = HiveJdbcWriter.jdbcClient(fileService.jdbcUrl, fileService.queueName)
          val partitionColName=partitionfolder.split("=")(0)
          val partitionDateWithName = partitionColName + "=" + partitiondate
          HiveJdbcWriter.partitionHandler(fileService.dbName, fileService.tablename, jdbcconn, fileService.droppartition,
            List(List(partitionDateWithName)), destpathpartition)
        }
      }
      if (fileService.dynamicJsonToCsv) {
        val inputDF = readDynamicJson(filePath.toString)
        inputDF.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(s"$destpathpartition")
      }
      else {
        val destPathPartitionWithFileName = destpathpartition + "/" + fileName
        val sourcePath = fileStatus.getPath
        val deletesource = fileService.deletesource
        val overwrite = fileService.overwrite

        log.info(s"destPathPartition where files will be moved is $destPathPartitionWithFileName from $sourcePath with  delete source parameter as $deletesource and overwrite parameter as $overwrite")
        withRetryAndThrowException(2, 180, "moveFile")(
          FileUtil.copy(fileSystem, fileStatus.getPath, fileSystem, new Path(destPathPartitionWithFileName) , fileService.deletesource, fileService.overwrite,
            sparkSession.sparkContext.hadoopConfiguration)
        )

        log.info(s"Files moved to dest path $destPathPartitionWithFileName are: " + fileStatus.getPath)
      }
      totFilesize
    }
    else {
      log.warn("file name starts with '.' and will be ignored")
      0
    }

  }

  @SuppressWarnings(Array("NullParameter"))
  def processTargz(fullpath: String, houtPath: String)(implicit spark: SparkSession): Unit = {
    log.info("Tar entry")
    log.info("Tar Name entry :" + FilenameUtils.getName(fullpath))
    val fileName1 = FilenameUtils.getName(fullpath)
    val tarNamesFolder = fileName1.substring(0, fileName1.indexOf('.'))
    log.info("Folder Name : " + tarNamesFolder)

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(fullpath)
    val is: FSDataInputStream = fs.open(path)
    val gzipIn = new GzipCompressorInputStream(is)
    val tarIn = new TarArchiveInputStream(gzipIn)
    var entry: TarArchiveEntry = Option(tarIn.getNextTarEntry).orNull
    var i = 1
    try {
      while (entry != null) {
        log.info("Processing tar entry number {}", i)
        unpackTarEntry(tarIn, entry, fs, houtPath)
        i = i + 1
        entry = Option(tarIn.getNextTarEntry).orNull
      }
    } catch {
      case e: IOException =>
        log.error(e.getMessage, e.getCause)
    } finally {
      tarIn.close()
      gzipIn.close()
      is.close()
    }
  }

  @throws[IOException]
  @SuppressWarnings(Array("VarCouldBeVal"))
  def unpackTarEntry(tarIn: TarArchiveInputStream, entry: TarArchiveEntry, fs: FileSystem, houtPath: String): Unit = {
    val entryName = entry.getName
    log.info("Tar archive member name  : " + entryName)

    if (entry.isDirectory) {
      log.info("Tar archive member is a directory, will attempt to create")
      val f = new File(entryName)
      val created = f.mkdir
      if (!created) {
        throw new IOException(s"Unable to create directory ${f.getAbsolutePath}, during extraction of archive contents.")
      } else {
        log.info("Successfully created new directory : {}", f.getAbsolutePath)
      }
    } else {
      log.info("Tar archive member is a file, will attempt to unarchive")
      val hdfswritepath = new Path(houtPath + "/" + entryName)
      val fos = fs.create(hdfswritepath, true)
      val bufferSize = 2048
      val dest = new BufferedOutputStream(fos)
      val data = new Array[Byte](bufferSize)
      Try {
        var count = 0
        while ( {
          count = tarIn.read(data, 0, bufferSize)
          count != -1
        }) {
          dest.write(data, 0, count)
        }
      } match {
        case Failure(e) =>
          dest.flush()
          dest.close()
          throw new IOException(e.getMessage, e.getCause)
        case Success(_) =>
          dest.flush()
          dest.close()
      }
    }
  }


  /**
   * Input:
   * {"626740482200":[{"IMPRESSION_2":3,"CAMPAIGN_STATUS":"COMPLETED","DATE":"2021-10-26"}],
   * {"12344":[{"IMPRESSION_2":10,"CAMPAIGN_STATUS":"COMPLETED","DATE":"2021-10-26"}]}
   * Output:
   * CAMPAIGN_ID,IMPRESSION_2,CAMPAIGN_NAME,DATE
   * 626740482200,3,COMPLETED,2021-10-26
   * 12344,10,COMPLETED,2021-10-26
   */
  //@SuppressWarnings(Array("all"))
  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def readDynamicJson(srcpath: String)(implicit spark: SparkSession): DataFrame = {

    val parseInputToMap = readTextFileToMap(srcpath)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val resultArrayLists = mapToList(parseInputToMap)
    listToDataFrame(resultArrayLists)
  }

  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def readDynamicJsonMultipleFiles(srcpath: String)(implicit spark: SparkSession): DataFrame = {

    val resultArrayLists = new util.ArrayList[String]
    val srcstatus: Array[FileStatus] = getFileStatusArray(srcpath, "")
    var totFilesize: BigInt = 0
    if (srcstatus.length > 0) {
      srcstatus.foreach(x => {
        log.info("Source files to be processed are: " + x.getPath)
        log.info("Size of source files to be processed is: " + x.getLen)
        val filepathstring = x.getPath.toString
        val filepath = new File(filepathstring)
        val filename = filepath.getName
        if (!filename.startsWith(".")) {
          totFilesize += x.getLen
          val srcfilenamepath = s"$srcpath/$filename"
          val parseInputToMap = readTextFileToMap(srcfilenamepath)
          val innerArray = mapToList(parseInputToMap)
          resultArrayLists.addAll(innerArray)
        }
      }
      )
    }
    listToDataFrame(resultArrayLists)
  }

  def hdfsSize(destpath: String, env: String): Int = {

    if (!env.equals(MarketingConstants.LOCAL_ENV)) {
      val totFilesizeString = Seq("/bin/sh", "-c", s"hadoop fs -du -s $destpath | cut -d ' ' -f1") !!
      val totFilesize = totFilesizeString.trim.toInt
      log.info("Total file size of the files in the dest path " + destpath + ":" + totFilesize)
      totFilesize
    }
    else {
      val totFilesizeString = Seq("/bin/sh", "-c", s"find $destpath  -ls" + "| awk '{sum +=$7} END {print sum}'") !!
      val totFilesize = totFilesizeString.trim.toInt
      log.info("Total file size of the files in the dest path " + destpath + " :" + totFilesize)
      totFilesize
    }
  }

  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def readTextFileToMap(srcpath: String)(implicit spark: SparkSession): Map[String, Any] = {
    val inputDf = FileReader.readFileToDataFrame(srcpath, "text")
    val inputRows = inputDf.select("value").collect().map(_.getString(0)).mkString(" ")
    JsonMethods.parse(inputRows).values.asInstanceOf[Map[String, Any]]
  }

  def mapToList(map: Map[String, Any]): util.ArrayList[String] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val resultArrayLists = new util.ArrayList[String]
    for (key <- map.keySet) {
      val parseMapToString = mapper.writeValueAsString(map.get(key))
      resultArrayLists.add(parseMapToString)
    }
    resultArrayLists
  }

  def listToDataFrame(list: util.ArrayList[String])(implicit spark: SparkSession): DataFrame = {
    val resultArrayString = list.toString
    val ParseResultArrayToJsonString = "[" + resultArrayString.replaceAll("[\\[\\]]", "") + "]"
    import spark.implicits._
    val ParseResultDf = Seq(ParseResultArrayToJsonString).toDS
    val inputDF = spark.read.json(ParseResultDf).select($"*")
    inputDF
  }

  def getFileSystem()(implicit spark: SparkSession): FileSystem = {
    org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }

  def getFileStatusArray(dir: String, filter: String)(implicit spark: SparkSession): Array[FileStatus] = {
    if (filter == "") {
      getFileSystem().listStatus(new Path(dir))
    } else {
      filterFileStatusArray(filter, dir)
    }
  }

  def filterFileStatusArray(filter: String, dir: String)(implicit sparkSession: SparkSession): Array[FileStatus] = {
    val sourceDir = getFileSystem().listStatus(new Path(dir))
    val filterDir = new util.ArrayList[FileStatus]
    sourceDir.foreach(f => {
      val fileName = new File(f.getPath.toString).getName
      if (fileName.toLowerCase().contains(filter.toLowerCase())) {
        filterDir.add(f)
      }
    })
    filterDir.toArray(Array.ofDim[FileStatus](filterDir.size()))
  }

  @throws[IOException]
  def createTouchFile(filePath: String)(implicit spark: SparkSession): Unit = {
    val fs = getFileSystem()
    val touchFileName = new Path(filePath)
    fs.create(touchFileName, true)
    log.info("Created touch file :", touchFileName.getName)
  }

  @throws[IOException]
  def deleteTouchFile(filePath: String)(implicit spark: SparkSession): Unit = {
    val fs = getFileSystem()
    val touchFileName = new Path(filePath)
    fs.delete(touchFileName, true)
    log.info("Deleted touch file :", touchFileName.getName)
  }

  /**
   * Delete hdfs files
   *
   * @param srcFilepath Will delete hdfs files
   */

  def deleteHdfs(srcFilepath: String)(implicit sparkSession: SparkSession): Boolean = {

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fs.delete(new Path(srcFilepath), false)
  }

  /**
   * Delete hidden files
   *
   * @param inputEphemeralPath Will delete all the hidden files(starting with ".") from the input path
   */

  def deleteHiddenFile(inputEphemeralPath: String): Unit = {
    val downloadDirectory = new File(inputEphemeralPath)
    for (f <- downloadDirectory.listFiles) {
      if (f.getName.startsWith(".")) f.delete
    }
  }

  def writeDQCheckData(dataSet: Dataset[Row], outputLocation: String): Unit = {
    dataSet.show(false)
    dataSet
      .dropDuplicates()
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outputLocation)
    log.info(s"Wrote failed data to : $outputLocation")
  }


  /**
   * creates  new directory or empties
   */

  def makeDir(pathToDir: String)(implicit spark: SparkSession): Unit = {
    val fsAndFullPathFromCwd = HdfsUtils.getFileSystemAndCurrentWorkingPath(pathToDir)
    val fullOutPath = fsAndFullPathFromCwd._2
    log.info(fullOutPath)
    val mkdirsSuccess: Boolean = fsAndFullPathFromCwd._1.mkdirs(new Path(fullOutPath), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ))
    if (mkdirsSuccess) {
      log.info("Successfully created new directory : " + fullOutPath)
    } else {
      throw new IOException("Failed to create new directory : " + fullOutPath)
    }
  }

  /**
   * copy file from one path to another
   */

  def copyFile(srcFilepath: String, destFilepath: String)(implicit spark: SparkSession): Unit = {

    makeDir(destFilepath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.copyFromLocalFile(new Path(srcFilepath), new Path(destFilepath))
  }

  /**
   * delete file from source path
   */

  def deleteFile(srcFilepath: String)(implicit spark: SparkSession): Boolean = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(srcFilepath), false)
  }

  def updateKelsaDIPCheckpoint(listedValidPaths: List[String], checkpointPath: String, priorCheckpoint: String = "")(sparkSession: SparkSession): Unit = {
    val maxProcessedPartition: String = if (listedValidPaths.nonEmpty) {
      listedValidPaths.max.split("=").last
    } else {
      if (priorCheckpoint.isEmpty) log.warn("No prior checkpoint or valid processed paths, skipping updating the checkpoint")
      priorCheckpoint
    }

    if (maxProcessedPartition.nonEmpty) {
      log.info(s"Latest partition read and will be checkpointed for the next run to start reading ephemeral from: $maxProcessedPartition")
      CompactorService.updateCheckPointPath(checkpointPath, maxProcessedPartition)(sparkSession)
    }
  }

  def emptyDir(pathToDir: String)(sparkSession: SparkSession): Unit = {
    val fsAndFullPathFromCwd = HdfsUtils.getFileSystemAndCurrentWorkingPath(pathToDir)(sparkSession)
    val fs = fsAndFullPathFromCwd._1
    val fullOutPath = fsAndFullPathFromCwd._2
    log.info(fullOutPath)
    val fullOutPathPath = new Path(fullOutPath)

    val delDirsSuccess: Boolean = if (fs.exists(fullOutPathPath)) fs.delete(fullOutPathPath, true) else true
    if (delDirsSuccess) {
      log.info("Successfully deleted directory : " + fullOutPath)
    } else {
      throw new IOException("Failed to delete directory : " + fullOutPath)
    }
  }
}
