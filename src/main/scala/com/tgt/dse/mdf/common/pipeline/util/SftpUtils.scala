package com.tgt.dse.mdf.common.pipeline.util

import com.jcraft.jsch.{ChannelSftp, Session}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.exceptions.{GeneralException, ListOfSftpFilesException}
import com.tgt.dse.mdf.common.pipeline.service.FileReader.readFileToDataFrame
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.{getFilePathFileNameExtensionFromFilePath, shellCmdRun}
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.withRetryAndThrowException
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.collection.mutable

object SftpUtils extends LoggingTrait {

  /**
   * Gets list of files from the sftpFileLocation in the sftp server along with file size in tuple
   *
   * @param sftpChannel      : ChannelSftp
   * @param sftpFileLocation : sftpFileLocation.Can be directory or directory with filename
   * @return list of tuple (sftp filename,sftp filesize)
   */
  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def getListOfSftpFiles(sftpChannel: ChannelSftp,
                         sftpFileLocation: String
                        ): List[(String, Long)] = {

    log.info("Inside getListOfSftpFiles method")
    var listOfSftpFiles: List[(String, Long)] = List()

    try {
      val sftpFileList = sftpChannel.ls(sftpFileLocation)
      //val sftpFileList = sftpChannel.ls

      log.info(sftpFileList.toArray.mkString(","))
      for (i <- 0 until sftpFileList.size()) {
        val file = sftpFileList.get(i).asInstanceOf[ChannelSftp.LsEntry].getFilename
        val fileSize = getSftpFileSize(sftpChannel, sftpFileLocation + "/" + file)
        listOfSftpFiles = listOfSftpFiles :+ (file, fileSize)
      }
    } catch {
      case e: Exception => throw ListOfSftpFilesException(s"Not able to list the files in the sftp or remote path $sftpFileLocation", e)
    }
    //    val sftpFileLocationPath = Paths.get(sftpFileLocation)
    //    val sftpFilePath = sftpFileLocationPath.getParent.toString
    log.info(s"File list available in the remote path $sftpFileLocation : $listOfSftpFiles")
    listOfSftpFiles
  }


  /**
   * Gets list of files from the sftpFileLocation recursively in the sftp server along with file size in tuple and also checks if
   * sftp file is already moved to hdfs.If yes then will skip that file
   *
   * @param sftpChannel                                   :  ChannelSftp
   * @param sftpFileLocation                              :  sftpFileLocation.Can be directory or directory with filename
   * @param isFileAlreadySftpCheck                        :  Defaulted to false.If set will check the sftp file if its already moved to hdfs which is indicated by parameter hdfsFileLocationToCheckIsFileAlreadySftpCheck
   * @param hdfsFileLocationToCheckIsFileAlreadySftpCheck : Defaulted to empty string.hdfs file location to keep track of files that are sftp
   * @return list of tuple (sftp filename,sftp filesize)
   */
  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def getListOfSftpFilesRecursively(sftpChannel: ChannelSftp,
                                    sftpFileLocation: String,
                                    isFileAlreadySftpCheck: Boolean = false,
                                    hdfsFileLocationToCheckIsFileAlreadySftpCheck: String = ""
                                   )(implicit spark: SparkSession): List[(String, Long)] = {

    log.info("Inside getListOfSftpFilesRecursively method")
    var listOfSftpFiles: List[(String, Long)] = List()
    val fileNamesAlreadySftp: Array[Any] = if (isFileAlreadySftpCheck) {
      log.info(s"Reading file in the path $hdfsFileLocationToCheckIsFileAlreadySftpCheck to get list of files already sftp")
      val fileNamesAlreadySftp: Array[Any] = readFileToDataFrame(
        fileLocation = hdfsFileLocationToCheckIsFileAlreadySftpCheck,
        fileFormat = "csv").collect.flatMap(_.toSeq)
      fileNamesAlreadySftp
    } else {
      Array[Any]()
    }

    try {
      val sftpFileList = sftpChannel.ls(sftpFileLocation)
      log.info(sftpFileList.toArray.mkString(","))
      for (i <- 0 until sftpFileList.size()) {
        if (!sftpFileList.get(i).asInstanceOf[ChannelSftp.LsEntry].getAttrs.isDir) {
          val file = sftpFileList.get(i).asInstanceOf[ChannelSftp.LsEntry].getFilename
          val fileSize = getSftpFileSize(sftpChannel, StringUtils.removeEnd(sftpFileLocation, "/") + "/" + file)
          val isFileAlreadySftp = checkIfFileIsAlreadySftp(isFileAlreadySftpCheck, file, fileNamesAlreadySftp)
          if (!isFileAlreadySftp) {
            listOfSftpFiles = listOfSftpFiles :+ (StringUtils.removeEnd(sftpFileLocation, "/") + "/" + file, fileSize)
          } else {

            log.info(s"file $file in the remote server is already present in the hdfs location $hdfsFileLocationToCheckIsFileAlreadySftpCheck,so will skip sftp the file")
          }
        }
        else {
          if (!sftpFileList.get(i).asInstanceOf[ChannelSftp.LsEntry].getFilename.equals(".") &&
            !sftpFileList.get(i).asInstanceOf[ChannelSftp.LsEntry].getFilename.equals("..")) {
            getListOfSftpFilesRecursively(sftpChannel,
              StringUtils.removeEnd(sftpFileLocation, "/") + "/" + sftpFileList.get(i).asInstanceOf[ChannelSftp.LsEntry].getFilename)
          }
        }
      }
    }
    catch {
      case e: Exception => throw ListOfSftpFilesException(s"Not able to list the files in the sftp or remote path $sftpFileLocation", e)
    }
    log.info(s"File list available in the remote path $sftpFileLocation : $listOfSftpFiles")
    listOfSftpFiles
  }

  /**
   * Checks if sftp file is already sftp to hdfs.Files that are already sftp will be passed as parameter as array of filenames
   *
   * @param checkExistingFilesAlreadySftp : Boolean.If set then will check else will return false
   * @param sftpFileName                  : sftp filename
   * @param existingFilesAlreadySftp      : Array of filenames that are already sftp
   * @return boolean
   */
  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def checkIfFileIsAlreadySftp(
                                checkExistingFilesAlreadySftp: Boolean,
                                sftpFileName: String,
                                existingFilesAlreadySftp: Array[Any]
                              ): Boolean = {


    log.info("list of files already sftp=============>" + existingFilesAlreadySftp.mkString(","))
    if (checkExistingFilesAlreadySftp) {
      if (existingFilesAlreadySftp.contains(sftpFileName)) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }


  /**
   * Read comma separated file from hdfs location and appends new value to and moves to the hdfs location
   *
   * @param valueToBeAppended            : list of values to be appended to hdfsFileLocationWithFileName
   * @param hdfsFileLocationWithFileName :  hdfs file name with path which will be appended with valueToBeAppended
   * @return Int
   */
  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def appendToFile(valueToBeAppended: List[String],
                   hdfsFileLocationWithFileName: String
                  )(implicit spark: SparkSession): Int = {

    val commaSepVal = readFileToDataFrame(
      fileLocation = hdfsFileLocationWithFileName,
      fileFormat = "csv").collect().mkString(",")

    val newCommaVal = commaSepVal.replaceAll("\\[", "").replaceAll("]", "") + "," + valueToBeAppended.mkString(",")

    val splitCmd = s"echo $newCommaVal | hadoop fs -put -f - ${hdfsFileLocationWithFileName}"

    val shellCmdStatus = shellCmdRun(splitCmd)

    shellCmdStatus
  }

  /**
   * List all files at the sftp location.
   *
   * @param sftpChannel   ChannelSftp
   * @param sftpDirectory sftpDirectory
   * @return List of files at sftp location.
   */
  def listFilesInSftp(sftpChannel: ChannelSftp, sftpDirectory: String): Seq[String] = {
    sftpChannel.ls(sftpDirectory).toArray.map(x => x.toString.split(" ").last).toList
    //disConnectSftpConnection(sftpChannel)
  }

  /**
   * Gets file size from the file in the sftpFileLocation
   *
   * @param sftpChannel      : ChannelSftp
   * @param sftpFileLocation : sftpFileLocation.sftp Directory with filename
   * @return file size
   */

  def getSftpFileSize(sftpChannel: ChannelSftp, sftpFileLocation: String): Long = {

    sftpChannel.lstat(sftpFileLocation).getSize
    //    val sftpFileList = sftpChannel.ls(sftpFileLocation)
    //    val sftpFileSize = sftpFileList.asInstanceOf[ChannelSftp#LsEntry].getAttrs.getSize

  }

  /**
   * Gets file modification time in the sftpFileLocation
   *
   * @param sftpChannel      : ChannelSftp
   * @param sftpFileLocation : sftpFileLocation.sftp Directory with filename
   * @return file size
   */

  def getSftpFileModificationTime(sftpChannel: ChannelSftp, sftpFileLocation: String): Long = {

    sftpChannel.lstat(sftpFileLocation).getMTime

  }

  /**
   * Compares initial file size of the file in sft server with current file size and also initial and current modification time.
   * If file size is same and modification time is same then we will return unit else will throw exception
   *
   * @param sftpChannel                  : ChannelSftp
   * @param sftpFileLocationWithFileName : sftpFileLocation.sftp Directory with filename
   * @param initialFileSizeOfTheFile     : initialFileSize of the sftpFile
   * @param initialFileModificationTime  : initialFileModificationTime of the sftpFile
   * @return unit
   */

  def getCurrentFileSizeAndCompareWithInitial(sftpChannel: ChannelSftp,
                                              sftpFileLocationWithFileName: String,
                                              initialFileSizeOfTheFile: Long,
                                              initialFileModificationTime: Long)
  : Unit = {


    val currentFileSizeOfTheFile = getSftpFileSize(sftpChannel, sftpFileLocationWithFileName)
    val currentFileModificationTime = getSftpFileModificationTime(sftpChannel, sftpFileLocationWithFileName)
    if (initialFileSizeOfTheFile == currentFileSizeOfTheFile && initialFileModificationTime == currentFileModificationTime) {
      log.info("FileSize is not changing in sftp server ,will sftp the file")

    }
    else {

      throw GeneralException("FileSize is changing in sftp server ,will wait and recheck if complete file upload is complete")
    }

  }

  /**
   * Compares initial file size of the file and modification time in sft server with current file size,.If file size and modification time is same then we will return unit else
   * will retry with sleeping between retries.WIll throw exception after retries
   *
   * @param sftpChannel                  : ChannelSftp
   * @param sftpFileLocationWithFileName : sftpFileLocation.sftp Directory with filename
   * @param initialFileSizeOfTheFile     : initialFileSize of the sftpFile
   * @param initialFileModificationTime  : initialFileModificationTime of the sftpFile
   * @param retries                      : Retry count
   * @param sleep                        : sleep
   * @return unit
   */

  def sftpFileSizeCheckForCompleteness(sftpChannel: ChannelSftp,
                                       sftpFileLocationWithFileName: String,
                                       initialFileSizeOfTheFile: Long,
                                       initialFileModificationTime: Long,
                                       retries: Int,
                                       sleep: Long)
  : Unit = {

    withRetryAndThrowException(retries, sleep, "sftpFileSizeCheckForCompleteness")(

      getCurrentFileSizeAndCompareWithInitial(sftpChannel, sftpFileLocationWithFileName, initialFileSizeOfTheFile, initialFileModificationTime)

    )

  }

  /**
   * Checks if sftp directory is present or not.
   *
   * @param sftpChannel          : ChannelSftp
   * @param sftpFileLocationPath : sftpFileLocationPath
   * @return Boolean
   */

  def checkSftpDirectoryPathExistence(sftpChannel: ChannelSftp, sftpFileLocationPath: String): Boolean = {

    try {
      log.info(s"sftpRemoteFileLocationPath ${sftpFileLocationPath}")
      sftpChannel.stat(sftpFileLocationPath)
      true
    }
    catch {
      case e: Exception =>
        sftpChannel.exit()
        sftpChannel.disconnect()
        log.info("checkSftpDirectoryPathExistence Exception==>" + e)
        false
      //throw new Exception(s"Directory not found in the sftp $sftpFileLocationPath: " + e.getMessage)
    }

  }

  /**
   * Checks if sftp file is present or not.
   *
   * @param sftpChannel          : ChannelSftp
   * @param sftpFileLocationPath : sftpFileLocationPath.sftp directory with filename
   * @return Boolean
   */

  def checkSftpFileExistence(sftpChannel: ChannelSftp, sftpFileLocationPath: String): Boolean = {

    val path = Paths.get(sftpFileLocationPath)
    val fileName = path.getFileName.toString
    val newSftpFileLocationPath = if (fileName.isEmpty || fileName == null) sftpFileLocationPath + "/" + "*" else sftpFileLocationPath
    val list = sftpChannel.ls(newSftpFileLocationPath)
    if (list.isEmpty) {
      return false
    }
    true
  }

  /**
   * Disconnect SFTP connections
   *
   * @param sftpChannel : ChannelSftp
   * @return Unit
   */
  def disConnectSftpConnection(sftpChannel: ChannelSftp): Unit = {

    val session: Session = sftpChannel.getSession()
    sftpChannel.exit()
    session.disconnect()
  }

  /**
   * Delete sftp file at the sftp location.
   *
   * @param sftpChannel ChannelSftp
   * @param SftpFile    sftpDirectory with filename
   * @return unit
   */

  def deleteFileInSftp(sftpChannel: ChannelSftp, SftpFile: String): Unit = {
    sftpChannel.rm(SftpFile)
    sftpChannel.disconnect()
  }

  /**
   * Delete list of sftp file at the sftp location.
   *
   * @param sftpChannel           ChannelSftp
   * @param listOfSftpFiles       listOfSftpFiles with filepath
   * @param sftpSearchFilePattern sftpSearchFilePattern
   * @return list of deleted sftp files
   */

  def deleteListOfFileInSftp(sftpChannel: ChannelSftp, listOfSftpFiles: List[String], sftpSearchFilePattern: String = ""): List[String] = {

    var sftpFileLocation = ""
    var listOfDeletedSftpFileLocation: List[String] = List()
    try {
      for (files <- listOfSftpFiles) {

        sftpFileLocation = files
        val sftpFileName = getFilePathFileNameExtensionFromFilePath(files)._2

        val sftpSearchFilePatternBool = if (sftpSearchFilePattern.nonEmpty && sftpFileName.contains(sftpSearchFilePattern)) {
          log.info(s"File in remote $sftpFileName  match with $sftpSearchFilePattern.This will be deleted")
          true
        }
        else if (sftpSearchFilePattern.isEmpty) {
          true
        }
        else {
          log.info(s"File in remote $sftpFileName does not match with $sftpSearchFilePattern.This will not be deleted")
          false
        }
        if (sftpSearchFilePatternBool) {
          deleteFileInSftp(sftpChannel, files)
          listOfDeletedSftpFileLocation = listOfDeletedSftpFileLocation :+ files
        }
      }
    } catch {
      case e: Exception => throw ListOfSftpFilesException(s"Not able to delete the file $sftpFileLocation in the sftp", e)
    }

    listOfDeletedSftpFileLocation
  }

}
