package com.tgt.dse.mdf.common.pipeline.service

import com.jcraft.jsch.{ChannelSftp, JSch}
import com.springml.sftp.client.SFTPClient
import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.util.{FileServiceHelper, KelsaUtil}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{SftpConnectionException, TransferFromHdfsToSftpException, TransferFromSftpToHdfsException}
import com.tgt.dse.mdf.common.pipeline.service.EmailService.sendEmailWrapper
import com.tgt.dse.mdf.common.pipeline.service.FileReader.readFileToDataFrame
import com.tgt.dse.mdf.common.pipeline.util.CredentialUtils.getCredentialFromJcek
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.{getFileNameAndExtensionFromFileName, getFilePathFileNameExtensionFromFilePath, getValue}
import com.tgt.dse.mdf.common.pipeline.util.HdfsUtils._
import com.tgt.dse.mdf.common.pipeline.util.SftpUtils.{disConnectSftpConnection, getListOfSftpFilesRecursively, getSftpFileModificationTime, getSftpFileSize, sftpFileSizeCheckForCompleteness}
import com.typesafe.config.{Config, ConfigException}
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}

/**
 * Case class needed to build SftpTransferConfig object to be used in SftpTransferService
 *
 * @param pipeLineConfig pipeLineConfig
 *                       Members
 *                       config:                            pipeLineConfig.config,
 *                       env:                               Read from MarketingConstants.APP_ENV,
 *                       jdbcURL:                           Read from MarketingConstants.APP_JDBCURL,
 *                       queueName:                         Read from MarketingConstants.APP_QUEUENAME,
 *                       sftpPasswordJceKeyStorePath:       Defaulted to empty string.If its password based then default sftpPrivateKeyPath to empty string.
 *                                                          Read from MarketingConstants.APP_SFTPPASSWORDJCEKEYSTOREPATH("app.sftpPasswordJceKeyStorePath"),
 *                       sftpPasswordJceKeyName:            Defaulted to empty string.If its password based then default sftpPrivateKeyPath to empty string
 *                                                          Read from MarketingConstants.APP_SFTPPASSWORDJCEKEYNAME("app.sftpPasswordJceKeyName"),
 *                       sftpPrivateKeyPath:                Defaulted to empty string.
 *                                                          If its key based then default sftpPasswordJceKeyStorePath,sftpPasswordJceKeyName to empty string
 *                                                          Read from MarketingConstants.APP_SFTPPRIVATEKEYPATH("app.sftpPrivateKeyPath"),
 *                       sftpUser:                          Read from MarketingConstants.APP_SFTPUSER("app.sftpUser"),
 *                       sftpHost:                          Read from MarketingConstants.APP_SFTPHOST("app.sftpHost"),
 *                       sftpPort:                          Read from MarketingConstants.APP_SFTPPORT("app.sftpPort"),
 *                       sftpFileLocationPath:              Sftp file location.Read from MarketingConstants.APP_SFTPFILELOCATIONPATH("app.sftpFileLocationPath"),
 *                       hdfsFileLocationPath:              HDFS file location.Read from MarketingConstants.APP_HDFSFILELOCATIONPATH("app.hdfsFileLocationPath"),
 *                       hdfsSearchFilePattern:              Defaulted to empty string.Read from MarketingConstants.APP_HDFSSEARCHFILEPATTERN("app.HdfsSearchFilePattern").
 *                                                          Used to search for hdfs file in the hdfsFileLocation based on this pattern
 *                       sftpSearchFilePattern:             Defaulted to empty string.Read from MarketingConstants.APP_SFTPSEARCHFILEPATTERN("app.sftpSearchFilePattern").
 *                                                          Filename search pattern to look for file in remote/sftp server
 *                       hdfsFileRenameFileName             Read from MarketingConstants.APP_HDFSFILERENAMEFILENAME("app.hdfsFileRenameFileName").
 *                                                          Defaulted to empty string.Hdfs rename file name.
 *                                                          Hdfs File name will be renamed to hdfsFileRenameFileName in the sftp server
 *                                                          If there are multiple files in hdfs which needs to be renamed while moving to sftp
 *                                                          format of rename will be filename_$i.fileextension.
 *                       copyMechanismType                  Defaulted to stream.Read from MarketingConstants.APP_COPYMECHANISMTYPE("app.copyMechanismType").
 *                                                          Values are download or if not specified will default to stream.
 *                                                          If download then will download to java temp dir and then moved to hdfs.
 *                                                          If not set to download then will stream the file from hdfs to sftp.
 *                       sftpSleep                          Defaulted to 0 millisecond.Read from MarketingConstants.APP_SFTPSLEEP("app.sftpSleep").
 *                                                          Will sleep before ftping the next file from hdfs to sftp server(not from sftp tp hdfs).
 *                       hdfsFileRenameFlag                 Defaulted to true.Read from MarketingConstants.APP_HDFSFILERENAMEFLAG("app.hdfsFileRenameFlag").
 *                                                          If set then will rename the file(hdfsFileRenameFileName) in the sftpFileLocation.Used in transferFromHdfsToSftp method.
 *                       sftpFileRenameFileName             Read from MarketingConstants.APP_SFTPFILERENAMEFILENAME("app.sftpFileRenameFileName").Defaulted to empty string.
 *                                                          sftp rename file name with extension.This is used in transferring file from sftp to hdfs
 *                                                          If set ,sftp File name will be renamed to sftpFileRenameFileName in the hdfs
 *                                                          If there are multiple files in sftp which needs to be renamed while moving to hdfs
 *                                                          format of rename will be filename_$i.fileextension else will use the filename in remote
 *                       fileCompletenessCheck              Defaulted to false.If set to true then will compares initial file size of the file in sft server with current file size.
 *                                                          and modified timestamp.If file size is same and modified timestamp is same then we will return unit else will retry based on retriesForFileCompletenessCheck parameter
 *                                                          with sleeping between retries based on sleepForFileCompletenessCheck parameter.
 *                                                          will throw exception after retries
 *                       retriesForFileCompletenessCheck    Defaulted to 2.Applicable if fileCompletenessCheck is set
 *                       sleepForFileCompletenessCheck      Defaulted to 3 min.Applicable if fileCompletenessCheck is set
 *                       isFileAlreadySftpCheck             Defaulted to false.Used while moving file from sftp to hdfs.If set will check the sftp file if its already moved to hdfs which is indicated by parameter hdfsFileLocationToCheckIsFileAlreadySftpCheck
 *                                                          If yes then will skip the file
 *                       hdfsFileLocationToCheckIsFileAlreadySftpCheck  Defaulted to empty string.Used while moving file from sftp to hdfs.hdfs file location to keep track of files that are sftp
 * @return SftpTransferConfig
 */
case class SftpTransferConfig(pipeLineConfig: PipeLineConfig) extends LoggingTrait {

  val config: Config = pipeLineConfig.config
  val env: String = config.getString(MarketingConstants.APP_ENV)
  val jdbcURL: String = config.getString(MarketingConstants.APP_JDBCURL)
  val queueName: String = config.getString(MarketingConstants.APP_QUEUENAME)
  val sftpPasswordJceKeyStorePath: String = try {
    config.getString(MarketingConstants.APP_SFTPPASSWORDJCEKEYSTOREPATH)
  } catch {
    case e: ConfigException => ""
  }

  val sftpPasswordJceKeyName: String = try {
    config.getString(MarketingConstants.APP_SFTPPASSWORDJCEKEYNAME)
  } catch {
    case e: ConfigException => ""
  }

  //may be make this mandatory
  val sftpPrivateKeyPath: String = try {
    config.getString(MarketingConstants.APP_SFTPPRIVATEKEYPATH)
  } catch {
    case e: ConfigException => ""
  }

  val sftpUser: String = config.getString(MarketingConstants.APP_SFTPUSER)
  val sftpHost: String = config.getString(MarketingConstants.APP_SFTPHOST)
  val sftpPort: Int = config.getInt(MarketingConstants.APP_SFTPPORT)
  val sftpFileLocationPath: String = config.getString(MarketingConstants.APP_SFTPFILELOCATIONPATH)
  val hdfsFileLocationPath: String = config.getString(MarketingConstants.APP_HDFSFILELOCATIONPATH)

  val hdfsSearchFilePattern: String = try {
    config.getString(MarketingConstants.APP_HDFSSEARCHFILEPATTERN)
  } catch {
    case e: ConfigException => ""
  }

  val sftpSearchFilePattern: String = try {
    config.getString(MarketingConstants.APP_SFTPSEARCHFILEPATTERN)
  } catch {
    case e: ConfigException => ""
  }

  val hdfsFileRenameFileName: String = try {
    config.getString(MarketingConstants.APP_HDFSFILERENAMEFILENAME)
  } catch {
    case e: ConfigException => ""
  }

  val copyMechanismType: String = try {
    config.getString(MarketingConstants.APP_COPYMECHANISMTYPE)
  } catch {
    case e: ConfigException => "stream"
  }

  val sftpSleep: Long = try {
    config.getLong(MarketingConstants.APP_SFTPSLEEP)
  } catch {
    case e: ConfigException => 0
  }

  val hdfsFileRenameFlag: Boolean = try {
    config.getBoolean(MarketingConstants.APP_HDFSFILERENAMEFLAG)
  } catch {
    case e: ConfigException => true
  }

  val sftpFileRenameFileName: String = try {
    config.getString(MarketingConstants.APP_SFTPFILERENAMEFILENAME)
  } catch {
    case e: ConfigException => ""
  }

  val fileCompletenessCheck: Boolean = try {
    config.getBoolean(MarketingConstants.APP_FILECOMPLETENESSCHECK)
  } catch {
    case e: ConfigException => false
  }

  val retriesForFileCompletenessCheck: Int = try {
    config.getInt(MarketingConstants.APP_RETRIESFORFILECOMPLETENESSCHECK)
  } catch {
    case e: ConfigException => 2
  }

  val sleepForFileCompletenessCheck: Long = try {
    config.getLong(MarketingConstants.APP_SLEEPFORFILECOMPLETENESSCHECK)
  } catch {
    case e: ConfigException => 3 * 60
  }

  val isFileAlreadySftpCheck: Boolean = try {
    config.getBoolean(MarketingConstants.APP_ISFILEALREADYSFTPCHECK)
  } catch {
    case e: ConfigException => false
  }

  val hdfsFileLocationToCheckIsFileAlreadySftpCheck: String = try {
    config.getString(MarketingConstants.APP_HDFSFILELOCATIONTOCHECKISFILEALREADYSFTPCHECK)
  } catch {
    case e: ConfigException => ""
  }

}

class SftpTransferService extends LoggingTrait {

  /**
   * Gets sftp secrets ,either using private key or passwords from jceks.If its password based authentication and sftpPasswordJceKeyName is not provided then
   * sftpPasswordJceKeyStorePath can be used to store the password in hdfs and this will read as is.
   *
   * @param sftpPasswordJceKeyStorePath sftpPasswordJceKeyStorePath.If its password based then default sftpPrivateKeyPath to empty string
   * @param sftpPasswordJceKeyName      sftpPasswordJceKeyName.If its password based then default sftpPrivateKeyPath to empty string
   * @param sftpPrivateKeyPath          sftpPrivateKeyPath.If its key based then default sftpPasswordJceKeyStorePath and sftpPasswordJceKeyName to empty string
   * @return path where private key is stored in jvm or passwords from jceks
   */

  def getSftpSecrets(sftpPasswordJceKeyStorePath: String,
                     sftpPasswordJceKeyName: String,
                     sftpPrivateKeyPath: String)(implicit SparkSession: SparkSession): String = {

    val sftpKeyValue =
      if (sftpPrivateKeyPath != "") {
        log.info(s"Reading sftp private key from the path $sftpPrivateKeyPath to connect to remote")
        val tempFolder = System.getProperty("java.io.tmpdir")
        val sftpKeyFileLocationPath = Paths.get(sftpPrivateKeyPath)
        val sftpKeyFilePath = sftpKeyFileLocationPath.getParent.toString
        val sftpKeyFileName = sftpKeyFileLocationPath.getFileName.toString
        localFileCopyFromHdfs(sftpPrivateKeyPath, tempFolder + "/" + sftpKeyFileName)
        tempFolder + "/" + sftpKeyFileName
      }
      else {
        if (sftpPasswordJceKeyName != "") {
          log.info(s"Reading sftp Jcek key store path $sftpPasswordJceKeyStorePath to connect to remote")
          getCredentialFromJcek(sftpPasswordJceKeyStorePath, sftpPasswordJceKeyName)
        }

        else {
          log.info(s"Reading secret stored in sftp Jcek key store path $sftpPasswordJceKeyStorePath to connect to remote")
          readFileToDataFrame(
            fileLocation = sftpPasswordJceKeyStorePath,
            fileFormat = "csv",
            getFileName = true).collect().map(_.getString(0)).mkString(" ")
        }
      }

    sftpKeyValue

  }

  /**
   * Creates sftp connection using JSch and ssh client using SFTPClient
   *
   * @param sftpPasswordJceKeyStorePath sftpPasswordJceKeyStorePath.If its password based then default sftpPrivateKeyPath to empty string
   * @param sftpPasswordJceKeyName      sftpPasswordJceKeyName.If its password based then default sftpPrivateKeyPath to empty string
   * @param sftpPrivateKeyPath          sftpPrivateKeyPath.If its key based then default sftpPasswordJceKeyStorePath and sftpPasswordJceKeyName to empty string
   * @param sftpUser                    sftpUser
   * @param sftpHost                    sftpHost
   * @param sftpPort                    sftpPort
   * @return (ChannelSftp,SFTPClient)
   */
  def createSftpConnection(sftpPasswordJceKeyStorePath: String,
                           sftpPasswordJceKeyName: String,
                           sftpPrivateKeyPath: String,
                           sftpUser: String,
                           sftpHost: String,
                           sftpPort: Int)(implicit SparkSession: SparkSession): (ChannelSftp, SFTPClient) = {

    val jsch = new JSch
    val sftpSecrets = getSftpSecrets(sftpPasswordJceKeyStorePath, sftpPasswordJceKeyName, sftpPrivateKeyPath)
    var sftpConnectionType = ""
    if (sftpPrivateKeyPath.nonEmpty) {
      val path = Paths.get(sftpSecrets)
      val bytes = Files.size(path)
      bytes match {
        case x if x > 0 => jsch.addIdentity(sftpSecrets); sftpConnectionType = "keyType"
        case _ => throw SftpConnectionException("sftpPrivateKey is empty")
      }
    }
    else {
      if (sftpPasswordJceKeyName.nonEmpty && sftpPasswordJceKeyStorePath.nonEmpty) {
        //jsch.addIdentity(sftpSecrets)
        sftpConnectionType = "passwordType"
      }
      else if (sftpPasswordJceKeyStorePath.nonEmpty) {
        //jsch.addIdentity(sftpSecrets)
        sftpConnectionType = "passwordType"
      }
      else {
        throw SftpConnectionException("No sftpPrivateKeyPath or sftpPasswordJceKeyStorePath provided in the application conf")
      }
    }
    //jsch.setKnownHosts(System.getProperty("user.home") + "/.ssh/known_hosts")
    val session = jsch.getSession(sftpUser, sftpHost, sftpPort)
    try {
      session.setConfig("StrictHostKeyChecking", "no")
      if (sftpConnectionType == "passwordType") {
        session.setPassword(sftpSecrets)
      }
      session.connect()
      log.info("Sftp session successful")

    } catch {
      case e: Exception => throw SftpConnectionException(s"Unable to connect to host $sftpHost with user $sftpUser " +
        s"and authentication based on $sftpConnectionType==>" + e)
    }
    val sftpChannel: ChannelSftp = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    try {
      sftpChannel.connect()
    } catch {
      case e: Exception =>
        session.disconnect()
        throw SftpConnectionException(s"Unable to connect to SFTP server with host $sftpHost " +
          s"with user $sftpUser and authentication based on $sftpConnectionType==>" + e)
    }
    if (sftpChannel.isConnected) {
      log.info("Sftp channel connection success")
    } else {
      session.disconnect()
      throw new Exception(s"Sftp Channel Not Connected for the host $sftpHost " +
        s"with user $sftpUser and authentication based on $sftpConnectionType")
    }
    val sftpPassword = if (sftpConnectionType == "keyType") null else sftpSecrets
    val sftpClient = new SFTPClient(getValue(Option(sftpSecrets)),
      getValue(None),
      sftpUser,
      sftpPassword,
      sftpHost,
      sftpPort
    )
    (sftpChannel, sftpClient)
  }

  /**
   * Recursively transfer file from HDFS location to SFTP location.If hdfs location contains sub directory then it copies files from sub directory as well
   *
   * @param sftpChannel            ChannelSftp
   * @param sftpFileLocation       Destination sftp/remote file location(directory)
   * @param hdfsFileLocation       Source Hdfs file location.Can be directory or directory with filename
   * @param hdfsFileRenameFileName Defaulted to empty string.Hdfs rename file name.
   *                               Hdfs File name will be renamed to hdfsFileRenameFileName in the sftp server
   *                               If there are multiple files in hdfs which needs to be renamed while moving to sftp
   *                               format of rename will be filename_$i.fileextension.
   * @param hdfsFileSearchPattern  Defaulted to empty string.Used to search for hdfs file in the hdfsFileLocation based on this pattern.
   * @param copyMechanismType      Values are download or if not specified will default to stream.
   *                               If download then will download to java temp dir and then moved to hdfs.
   *                               If not set to download then will stream the file from hdfs to sftp
   * @param sftpSleep              Defaulted to 0 millisecond.Will sleep before ftping the next file
   * @param hdfsFileRenameFlag     Defaulted to true.If set then will rename the file(hdfsFileRenameFileName) in the sftpFileLocation
   * @return list of tuple with (hdfs filename,hdfsfilesize,sftp filename,sftp filesize)
   */
  def transferFromHdfsToSftp(sftpChannel: ChannelSftp,
                             sftpFileLocation: String,
                             hdfsFileLocation: String,
                             hdfsFileRenameFileName: String = "",
                             hdfsFileSearchPattern: String = "",
                             copyMechanismType: String = "stream",
                             sftpSleep: Long = 0,
                             hdfsFileRenameFlag: Boolean = true
                            )(implicit sparkSession: SparkSession): List[(String, Long, String, Long)] = {

    log.info("Inside transferFromHdfsToSftp method")
    var listOfHdfsAndRemoteFilesWithFileSize: List[(String, Long, String, Long)] = List()
    var i = 1
    val localFilePath = System.getProperty("java.io.tmpdir")
    val listOfHdfsFilesQualified = listOfQualifiedHdfsFiles(hdfsFileLocation, hdfsFileSearchPattern)
    val listOfHdfsFileLocations = listOfHdfsFilesQualified._2
    val listOfHdfsFileName = listOfHdfsFilesQualified._1
    log.info(s"List of files that will be sftped from the hdfs path $hdfsFileLocation matching the hdfs file Search pattern $hdfsFileSearchPattern" +
      s" => $listOfHdfsFileName")
    //add size check of the file
    if (listOfHdfsFileName.nonEmpty) {
      for (files <- listOfHdfsFileLocations) {
        // or add here add size check of the file
        val hdfsFileLocation = files
        val hdfsFileName = getFilePathFileNameExtensionFromFilePath(hdfsFileLocation)._2
        val hdfsFileSize = getHdfsFileSize(hdfsFileLocation)
        val listOfHdfsFileNameSize = listOfHdfsFileName.length
        val renamedHdfsFileName = if (hdfsFileRenameFlag) {
          listOfHdfsFileNameSize match {
            case x if x > 1 && hdfsFileRenameFileName.nonEmpty =>
              s"${getFileNameAndExtensionFromFileName(hdfsFileRenameFileName)._1}_$i.${getFileNameAndExtensionFromFileName(hdfsFileRenameFileName)._2}"
            case x if x == 1 && hdfsFileRenameFileName.nonEmpty =>
              hdfsFileRenameFileName
            case _ => hdfsFileName
          }
        } else {
          hdfsFileName
        }
        val newSftpFileLocation = s"${StringUtils.removeEnd(sftpFileLocation, "/")}/$renamedHdfsFileName"
        val localFileLocation = localFilePath + "/" + renamedHdfsFileName
        log.info(s"File that will be moved to remote or sftp location=> $newSftpFileLocation from hdfs location $hdfsFileLocation")
        try {
          if (copyMechanismType == "download") {
            localFileCopyFromHdfs(hdfsFileLocation, localFilePath)
            val localFileLocationFile = new File(localFileLocation)
            val inputStream = new FileInputStream(localFileLocationFile)
            sftpChannel.put(inputStream, newSftpFileLocation)
            log.info(s"File successfully moved to remote or sftp location=> $newSftpFileLocation from hdfs location $hdfsFileLocation")
            inputStream.close()
            FileServiceHelper.deleteFile(localFileLocation)
          } else {
            val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
            val inputStream = fs.open(new Path(hdfsFileLocation))
            sftpChannel.put(inputStream, newSftpFileLocation)
            log.info(s"File successfully moved to remote or sftp location=> $newSftpFileLocation from hdfs location $hdfsFileLocation")
            inputStream.close()
          }
        }
        catch {
          case e: Exception =>
            disConnectSftpConnection(sftpChannel)
            throw TransferFromHdfsToSftpException(s"Transfer failed from hdfs ${hdfsFileLocation} to remote $sftpFileLocation" + e)
        }
        //val listOfSftpFiles = getListOfSftpFiles(sftpChannel,"/")
        val sftpFileSize = getSftpFileSize(sftpChannel, newSftpFileLocation)
        listOfHdfsAndRemoteFilesWithFileSize = listOfHdfsAndRemoteFilesWithFileSize :+ (hdfsFileName, hdfsFileSize, renamedHdfsFileName, sftpFileSize)
        i = i + 1
        log.info(s"Sleep for $sftpSleep millisecond before sftp'ing another file if any")
        Thread.sleep(sftpSleep)
      }
    }
    else {
      log.info(s"There are no files in hdfs path in the path $hdfsFileLocation  to be moved to sftp server")
    }
    listOfHdfsAndRemoteFilesWithFileSize
  }

  /**
   * Recursively transfer file from sftp server to hdfs.If sftp location contains sub directory then it copies files from sub directory as well
   *
   * @param sftpConnection                                (ChannelSftp,SFTPClient)
   * @param sftpFileLocation                              Source sftp/remote file location.Can be directory or directory with filename
   * @param hdfsFileLocation                              Destination hdfs file location.Can be directory or directory with filename
   * @param sftpSearchFilePattern                         Defaulted to empty string.Filename search pattern to look for file in remote/sftp server
   * @param copyMechanismType                             Values are download or if not specified will default to stream.
   *                                                      If download then will download to java temp dir and then moved to sftp.
   *                                                      If not set to download then will stream the file from sftp to hdfs
   * @param fileCompletenessCheck                         Defaulted to false.If set to true then will compares initial file size of the file in sft server with current file size.
   *                                                      If file size is same then we will return unit else will retry based on retriesForFileCompletenessCheck parameter
   *                                                      with sleeping between retries based on sleepForFileCompletenessCheck parameter.
   *                                                      will throw exception after retries
   * @param retriesForFileCompletenessCheck               Defaulted to 2.Applicable if fileCompletenessCheck is set
   * @param sleepForFileCompletenessCheck                 Defaulted to 3 mins.Applicable if fileCompletenessCheck is set
   * @param isFileAlreadySftpCheck                        Defaulted to false.Used while moving file from sftp to hdfs.If set will check the sftp file if its already moved to hdfs which is indicated by parameter hdfsFileLocationToCheckIsFileAlreadySftpCheck.
   *                                                      If yes then will skip the file
   * @param hdfsFileLocationToCheckIsFileAlreadySftpCheck Defaulted to empty string.Used while moving file from sftp to hdfs.hdfs file location to keep track of files that are sftp
   * @return list of tuple with (sftp filename,sftp fileSize,hdfs filename,hdfsFileSize)
   */
  @SuppressWarnings(Array("CyclomaticComplexity"))
  def transferFromSftpToHdfs(sftpConnection: (ChannelSftp, SFTPClient),
                             sftpFileLocation: String,
                             hdfsFileLocation: String,
                             sftpFileRenameFileName: String = "",
                             sftpSearchFilePattern: String = "",
                             copyMechanismType: String = "stream",
                             fileCompletenessCheck: Boolean = false,
                             retriesForFileCompletenessCheck: Int = 2,
                             sleepForFileCompletenessCheck: Long = 3 * 6000,
                             isFileAlreadySftpCheck: Boolean = false,
                             hdfsFileLocationToCheckIsFileAlreadySftpCheck: String = ""
                            )(implicit sparkSession: SparkSession): Seq[(String, Long, String, Long)] = {

    log.info("Inside transferFromSftpToHdfs")
    val sftpChannel = sftpConnection._1
    val sshClient = sftpConnection._2
    var listOfRemoteAndHdfsFilesWithFileSize: List[(String, Long, String, Long)] = List()
    var listOfRemoteFiles: List[String] = List()
    val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
    var i = 1
    //add recursive getListOfSftpFilesRecursively
    //val listOfSftpFilesQualified = getListOfSftpFiles(sftpChannel,sftpFileLocation)
    val listOfSftpFilesQualified = getListOfSftpFilesRecursively(sftpChannel, sftpFileLocation, isFileAlreadySftpCheck, hdfsFileLocationToCheckIsFileAlreadySftpCheck)
    if (listOfSftpFilesQualified.nonEmpty) {
      listOfSftpFilesQualified.foreach(file => {
        val sftpFileLocation = file._1
        val sftpFileName = getFilePathFileNameExtensionFromFilePath(file._1)._2
        val sftpFileSize = file._2
        val listOfSftpFilesSize = listOfSftpFilesQualified.length
        val sftpFileModificationTime = getSftpFileModificationTime(sftpChannel, sftpFileLocation)
        if (fileCompletenessCheck) {
          log.info("Checking for file completeness.Will sleep for a minute before checking for file completeness")
          Thread.sleep(2 * 30000)
          sftpFileSizeCheckForCompleteness(sftpChannel, sftpFileLocation, sftpFileSize, sftpFileModificationTime, retriesForFileCompletenessCheck, sleepForFileCompletenessCheck)
        }
        val renamedSftpFileName = listOfSftpFilesSize match {
          case x if x > 1 && sftpFileRenameFileName.nonEmpty =>
            s"${getFileNameAndExtensionFromFileName(sftpFileRenameFileName)._1}_$i.${getFileNameAndExtensionFromFileName(sftpFileRenameFileName)._2}"
          case x if x == 1 && sftpFileRenameFileName.nonEmpty =>
            sftpFileRenameFileName
          case _ => sftpFileName
        }
        val sftpSearchFilePatternBool = if (sftpSearchFilePattern.nonEmpty && sftpFileName.contains(sftpSearchFilePattern)) {
          log.info(s"File in remote $sftpFileName  match with $sftpSearchFilePattern.This will be moved to hdfs")
          true
        }
        else if (sftpSearchFilePattern.isEmpty) {
          true
        }
        else {
          log.info(s"File in remote $sftpFileName does not match with $sftpSearchFilePattern.This will not be moved to hdfs")
          false
        }
        if (sftpSearchFilePatternBool) {
          val newHdfsFileLocation = StringUtils.removeEnd(hdfsFileLocation, "/") + "/" + renamedSftpFileName
          try {
            if (copyMechanismType == "download") {
              val localFileLocation = System.getProperty("java.io.tmpdir") + "/" + sftpFileName
              val localFileDownloadFileLocation = sshClient.copy(sftpFileLocation, localFileLocation)
              log.info(s" File from remote path $sftpFileLocation is copied to local path $localFileDownloadFileLocation")
              localFileCopyToHdfs(localFileLocation, newHdfsFileLocation)
              FileServiceHelper.deleteFile(localFileLocation)
            } else {
              val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
              val outputStream: FSDataOutputStream = fs.create(new org.apache.hadoop.fs.Path(newHdfsFileLocation), true)
              log.info(s" File from remote path $sftpFileLocation will be moved to hdfs path $newHdfsFileLocation")
              sftpChannel.get(sftpFileLocation, outputStream)
              outputStream.close()
            }
          }
          catch {
            case e: Exception =>
              disConnectSftpConnection(sftpChannel)
              throw TransferFromSftpToHdfsException(s"Transfer failed from sftp $sftpFileLocation to HDFS $hdfsFileLocation==>" + e)
          }
          val hdfsFileSize = getHdfsFileSize(newHdfsFileLocation)
          val extension = FilenameUtils.getExtension(newHdfsFileLocation)
          val newHdfsFileName = if (FilenameUtils.getExtension(newHdfsFileLocation).isEmpty) {
            FilenameUtils.getBaseName(newHdfsFileLocation)
          }
          else {
            FilenameUtils.getBaseName(newHdfsFileLocation) + "." + extension
          }
          listOfRemoteAndHdfsFilesWithFileSize = listOfRemoteAndHdfsFilesWithFileSize :+ (sftpFileName, sftpFileSize, newHdfsFileName, hdfsFileSize)
          listOfRemoteFiles = listOfRemoteFiles :+ sftpFileLocation
        }
        i = i + 1
      })
      //delete file based on listOfRemoteFiles from sftp server based on sftp delete flag
      //deleteListOfFileInSftp(sftpChannel,listOfRemoteFiles)
      appendToHDFSFile(hdfsFileLocationToCheckIsFileAlreadySftpCheck, listOfRemoteFiles.mkString(","))
    }
    else {
      log.info(s"There are no files in sftp server in the path $sftpFileLocation  to be moved to hdfs path")
    }
    fs.close()
    disConnectSftpConnection(sftpChannel)
    listOfRemoteAndHdfsFilesWithFileSize

  }
}

object SftpTransferService extends LoggingTrait {

  /**
   * Creates SftpTransferConfig object(check SftpTransferConfig type for parameters to be specified in conf) which creates sftp connection and calls transferFromHdfsToSftp to transfer file from HDFS to SFTP
   * Recursively transfer file from HDFS location to SFTP location.If hdfs location contains sub directory then it copies files from sub directory as well
   *
   * @param pipeLineConfig PipeLineConfig object
   * @return unit
   */

  def transferFromHdfsToSftpWrapper(pipeLineConfig: PipeLineConfig)(implicit sparkSession: SparkSession): Unit = {

    log.info("Inside transferFromHdfsToSftpWrapper")
    val sftpTransferService = new SftpTransferService
    val sftpTransferConfig = SftpTransferConfig(pipeLineConfig)
    val sftpChannel: ChannelSftp = sftpTransferService.createSftpConnection(sftpTransferConfig.sftpPasswordJceKeyStorePath,
      sftpTransferConfig.sftpPasswordJceKeyName,
      sftpTransferConfig.sftpPrivateKeyPath,
      sftpTransferConfig.sftpUser,
      sftpTransferConfig.sftpHost,
      sftpTransferConfig.sftpPort)._1
    try {
      val listOfHdfsAndRemoteFilesWithFileSize = sftpTransferService.transferFromHdfsToSftp(sftpChannel,
        sftpTransferConfig.sftpFileLocationPath,
        sftpTransferConfig.hdfsFileLocationPath,
        sftpTransferConfig.hdfsFileRenameFileName,
        sftpTransferConfig.hdfsSearchFilePattern,
        sftpTransferConfig.copyMechanismType,
        sftpTransferConfig.sftpSleep,
        sftpTransferConfig.hdfsFileRenameFlag)
      log.info(s"List of Hdfs and Sftp files and file size moved from hdfs to sftp ${listOfHdfsAndRemoteFilesWithFileSize.mkString(",")}")
      disConnectSftpConnection(sftpChannel)
      //can add if there no files throw exception
    } catch {
      case e: Exception =>
        disConnectSftpConnection(sftpChannel)
        throw TransferFromHdfsToSftpException(s"Failed to push from HDFS path ${sftpTransferConfig.hdfsFileLocationPath}" +
          s" to remote path ${sftpTransferConfig.sftpFileLocationPath}==>" + e)
    }
  }

  /**
   * Creates SftpTransferConfig object(check SftpTransferConfig type for parameters to be specified in conf)
   * which creates sftp connection and calls transferFromSftpToHdfs to transfer file from SFTP to HDFS
   * Recursively transfer file from sftp server to hdfs.If sftp location contains sub directory then it copies files from sub directory as well
   *
   * @param pipeLineConfig PipeLineConfig object
   * @return Unit
   */
  def transferFromSftpToHdfsWrapper(pipeLineConfig: PipeLineConfig)(implicit sparkSession: SparkSession): Unit = {

    log.info("Inside transferFromSftpToHdfsWrapper")

    val sftpTransferService = new SftpTransferService
    val sftpTransferConfig = SftpTransferConfig(pipeLineConfig)
    val sftpConnection: (ChannelSftp, SFTPClient) = sftpTransferService.createSftpConnection(sftpTransferConfig.sftpPasswordJceKeyStorePath,
      sftpTransferConfig.sftpPasswordJceKeyName,
      sftpTransferConfig.sftpPrivateKeyPath,
      sftpTransferConfig.sftpUser,
      sftpTransferConfig.sftpHost,
      sftpTransferConfig.sftpPort)
    try {
      val listOfRemoteAndHdfsFilesWithFileSize = sftpTransferService.transferFromSftpToHdfs(sftpConnection,
        sftpTransferConfig.sftpFileLocationPath,
        sftpTransferConfig.hdfsFileLocationPath,
        sftpTransferConfig.sftpFileRenameFileName,
        sftpTransferConfig.sftpSearchFilePattern,
        sftpTransferConfig.copyMechanismType,
        sftpTransferConfig.fileCompletenessCheck,
        sftpTransferConfig.retriesForFileCompletenessCheck,
        sftpTransferConfig.sleepForFileCompletenessCheck,
        sftpTransferConfig.isFileAlreadySftpCheck,
        sftpTransferConfig.hdfsFileLocationToCheckIsFileAlreadySftpCheck)
      log.info(s"List of Sftp and Hdfs files and file size moved from Sftp to Hdfs ${listOfRemoteAndHdfsFilesWithFileSize.mkString(",")}")
      disConnectSftpConnection(sftpConnection._1)
      //can add if there no files throw exception
    } catch {
      case e: Exception =>
        disConnectSftpConnection(sftpConnection._1)
        throw TransferFromSftpToHdfsException(s"Failed to push from remote ${sftpTransferConfig.sftpFileLocationPath}" +
          s" to HDFS ${sftpTransferConfig.hdfsFileLocationPath}=>" + e)
    }


  }

  /**
   * Will save DF to HDFS path and calls transferFromHdfsToSftpWrapper to transfer file from HDFS to SFTP and send email
   *
   * @param pipeLineConfig              pipeLineConfig needed to build SftpTransferConfig object.
   * @param sftpDF                      Dataframe which needs to be saved to HDFS and then transferred to remote
   * @param sftpSelectColumns           select Columns from sftpDF
   * @param sftpFilterCondition         filter Condition on sftpDF
   * @param sftpFilename                Filename to be used when placing the file in the remote.
   *                                    This filename will be used to rename file in hdfs before moving to remote.
   *                                    If there are multiple files in sftpHdfsTempFileLocation which needs to be renamed while moving to hdfs
   *                                    format of rename will be filename_$i.fileextension else will use the filename in  sftpHdfsTempFileLocation
   * @param sftpHdfsTempFileLocation    Temp Hdfs location where the DF will stored.
   * @param sftpHdfsFileLocation        File in sftpHdfsTempFileLocation will be moved to this hdfs location
   *                                    after renaming the file in sftpHdfsTempFileLocation to sftpFilename
   * @param hdfsSaveOptions             Options to be used while saving DF to HDFS.like
   *                                    Map("header"->"true","sep"->",","mapreduce.fileoutputcommitter.marksuccessfuljobs"->"false")
   * @param hdfsSaveMode                Defaulted to overwrite to be used while saving DF to HDFS
   * @param hdfsSaveFileFormat          Defaulted to csv to be used while saving DF to HDFS
   * @param hdfsSaveCoalesce            Defaulted to  1 to be used while saving DF to HDFS
   * @param hdfsRenameFileSearchPattern Defaulted to empty string.Based on the pattern, file in searched in the path sftpHdfsTempFileLocation
   *                                    and this will be renamed before moving to sftpHdfsFileLocation
   * @param deleteHdfsFileSearchPattern Defaulted to *.Based on the pattern file will be deleted from sftpHdfsFileLocation
   *                                    before renaming file in sftpHdfsTempFileLocation to sftpHdfsFileLocation
   * @param deleteHdfsFileRecursive     Defaulted to false.If set,file will be deleted recursively from sftpHdfsFileLocation
   *                                    before renaming file in sftpHdfsTempFileLocation to sftpHdfsFileLocation
   * @param sendEmailAggregateSftpDF    Boolean.Email related parameter.Defaulted to false.If set then sftpDF will be aggregated based on the sqlStatement parameter
   * @param sendEmailAggregateSql       Defaulted to empty string.Email related parameter.If aggregateSftpDF set to true then need to provide sql statment to aggregate
   *                                    which will be emailed
   * @param sendEmailService            EmailService object.Defaulted to empty object
   * @param emailOnly                   Defaulted to false.If set ,will only send email about the stats based on sendEmailAggregateSql but will not transfer file
   * @return Unit
   */
  @SuppressWarnings(Array("MaxParameters"))
  def saveFromDFToHdfsAndTransferFromHdfsToSftpWrapper(
                                                        pipeLineConfig: PipeLineConfig,
                                                        sftpDF: DataFrame,
                                                        sftpSelectColumns: Seq[String] = Seq.empty[String],
                                                        sftpFilename: String,
                                                        sftpHdfsTempFileLocation: String,
                                                        sftpHdfsFileLocation: String,
                                                        sftpFilterCondition: String = "",
                                                        hdfsSaveOptions: Map[String, String],
                                                        hdfsSaveMode: String = "overwrite",
                                                        hdfsSaveFileFormat: String = "csv",
                                                        hdfsSaveCoalesce: Int = 1,
                                                        hdfsRenameFileSearchPattern: String = "",
                                                        deleteHdfsFileSearchPattern: String = "*",
                                                        deleteHdfsFileRecursive: Boolean = false,
                                                        sendEmail: Boolean = false,
                                                        sendEmailAggregateSftpDF: Boolean = false,
                                                        sendEmailAggregateSql: String = "",
                                                        sendEmailService: EmailService = EmailService("", "", "", "", "", attachDFResult = false, attachDFResultToEmailBody = false, debug = "false"),
                                                        emailOnly: Boolean = false
                                                      )
                                                      (implicit sparkSession: SparkSession): Unit = {
    if (!emailOnly) {
      val sftpCountDF = sftpDF.count()
      if (sftpCountDF > 0) {
        val sftpFilterDF: DataFrame = if (sftpFilterCondition.nonEmpty) sftpDF.where(sftpFilterCondition) else sftpDF
        val sftpSelectColDF = if (sftpSelectColumns.nonEmpty) sftpFilterDF.select(sftpSelectColumns.map(name => col(name)): _*) else sftpFilterDF
        hdfsSaveFromDF(sftpSelectColDF, writeMode = hdfsSaveMode, fileFormat = hdfsSaveFileFormat, fileSaveLocation = sftpHdfsTempFileLocation, coalesce = hdfsSaveCoalesce, options = hdfsSaveOptions)
        log.info(s"Renaming the file in the $sftpHdfsTempFileLocation")
        val hdfsFileRenameList = hdfsFileRename(sftpHdfsTempFileLocation, sftpHdfsFileLocation, sftpFilename, hdfsRenameFileSearchPattern, deleteHdfsFileSearchPattern, deleteHdfsFileRecursive)
        if (hdfsFileRenameList.length >= 1) {
          val fs: FileSystem = KelsaUtil.hadoopConfigurations(sparkSession)
          log.info(s"File ${sftpHdfsFileLocation}/$hdfsFileRenameList will be transferred to File express")
          //if (fs.exists(new Path(sftpHdfsFileLocation + "/" + sftpFilename))) {
          transferFromHdfsToSftpWrapper(pipeLineConfig)
          //}
        } else {
          log.info(s"There are no files in hdfs in the path ${sftpHdfsFileLocation}  to be moved to FileExpress." +
            "Check the path $sftpHdfsTempFileLocation for the file created by the input dataframe that is passed." +
            "If yes then there may be issue with renaming file in the directory sftpHdfsFileLocation." +
            "Check the parameters sftpHdfsTempFileLocation, sftpHdfsFileLocation, sftpFilename, hdfsRenameFileSearchPattern, deleteHdfsFileSearchPattern, deleteHdfsFileRecursive ")
          //throw error?
        }
      }
      else {
        log.error("SFTP Dataframe is empty so no file is transferred to FileExpress")
      }
    }
    // read from the FE path rather than from sftpDF if hdfs search pattern is giving?
    sendEmailWrapper(sftpDF, sendEmail, sendEmailAggregateSftpDF, sendEmailAggregateSql, sendEmailService)
  }

  /**
   * Example to write file to edgenode then transfer file to sftp server
   * log.info(s"Write file in the edge node path ${pipeLineService.irGuestStatusExportFolder} which has only changed guest status")
   * val guestStatusDeltaDF = filterChangedGuestStatusDF
   * .select(col("action_id"), col("order_id"), col("action_tracker_id"), col("revenue_a"), lit("OTHER").alias("Reason"), col("guest_status"))
   * dataFrameToEdgeNodeAndConsolidateAddHeader(guestStatusDeltaDF,
   * edgeNodeWritePath = pipeLineService.irGuestStatusExportTempFolder,
   * edgeNodeRenamePath=pipeLineService.irGuestStatusExportFolder,
   * header="ActionId,OID,ActionTrackerId,Amount,Reason,CustomerStatus",
   * renameFilename=s"target_customer_status_${currentDate}.csv"
   * )
   *
   * transferFromSftpToHdfsWrapper(pipeLineService.pipeLineConfig)
   */
}