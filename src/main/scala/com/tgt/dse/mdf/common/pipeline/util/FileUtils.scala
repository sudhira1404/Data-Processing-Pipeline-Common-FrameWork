package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.withRetryAndThrowException
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession

import scala.sys.process._

object FileUtils extends LoggingTrait {


  /**
   * Will get filename ,extension from filename
   * @param hdfsFileName                    hdfsFileName
   * @return (fileName,extension)
   */


  def getFileNameAndExtensionFromFileName(hdfsFileName: String):(String,String) = {

    //    val fileNameNoExtension = FilenameUtils.removeExtension(hdfsFileName)
    //    val fileNameExtension = hdfsFileName.substring(hdfsFileName.lastIndexOf(".") + 1)
    val extension = FilenameUtils.getExtension(hdfsFileName)
    val fileName = FilenameUtils.getBaseName(hdfsFileName)

    (fileName,extension)

  }

  /**
   * Will get path,filename ,extension from filename
   * @param fileLocation                    location of file with filename
   * @return (path,fileName,extension)
   */

  def getFilePathFileNameExtensionFromFilePath(fileLocation: String):(String,String,String) = {

    val newFileLocation = if (fileLocation.charAt(0).toString != "/") "/" + fileLocation else fileLocation
    val path = FilenameUtils.getPath(newFileLocation)
    val extension = FilenameUtils.getExtension(newFileLocation)
    val fileName = if (FilenameUtils.getExtension(newFileLocation).isEmpty)
      {
        FilenameUtils.getBaseName(newFileLocation)
      }

    else
      {
        FilenameUtils.getBaseName(newFileLocation) + "." + extension
      }


    (path,fileName,extension)

  }

  /**
   * Will get value based on parameter Option else empty string.Usage getValue(Option(sftpSecrets))
   * @param param       parameter
   * @return parmeter
   */

  def getValue(param: Option[String]): String = {
    if (param != null && param.isDefined) {
      param.getOrElse("")
    } else {
      null
    }
  }

  /**
   * Will run shell cmd
   * @param shellCmd                       shellCmd
   * @param retryCount                     Defaulted to 3.retryCount
   * @param retrySleep                     Defaulted to 3000.retrySleep
   * @return exit status
   */

  def shellCmdRun(shellCmd: String,retryCount:Int=3,retrySleep:Long = 3 * 1000):Int = {

    log.info(s"Executing shellCmd=>$shellCmd")
    val shellCmdExitStatus:Int = withRetryAndThrowException(retryCount, retrySleep, "shellCmdRun","Exception thrown in the function shellCmdRun")(

      {
        Seq("/bin/sh", "-c", s"""$shellCmd  """).!

      }
    )
    shellCmdExitStatus
  }

  /**
   * Will split the file(inputFilename) in path edgeNodeWritePath based on splitFilePrefix and splitNumberOfRecords and will add extension(fileFormat) to split file and add header(header) to split file and moves to edgeNodeSplitPath
   * @param edgeNodeWritePath                  edge node path where the input file is located
   * @param edgeNodeSplitPath                  split edge node path where the split file is located
   * @param splitFilePrefix                    split filename without extension
   * @param splitNumberOfRecords               split based on number of records
   * @param splitFileExtension                 split filename without extension
   * @param header                             Header to be added to split files
   * assume inputFilename="cust_status.csv" has 3 records
   * splitFilePrefix="cust_status"
   * splitNumberOfRecords=3
   * splitFileExtension="csv"
   * header="id,name"
   * split filename will be cust_statusaa.csv,cust_statusab.csv,cust_statusac.csv with id,name has header
   */

  def splitFileInEdgeNodeAndAddFileAddHeader(edgeNodeWritePath:String,
                                             edgeNodeSplitPath:String,
                                             splitFilePrefix:String,
                                             splitNumberOfRecords:Int,
                                             splitFileExtension:String,
                                             header:String): Int = {

    var shellCmdStatus=0
    val shellCmdRenameInputFile = s"cd $edgeNodeWritePath;cat ${edgeNodeWritePath}/* > ${edgeNodeWritePath}/s$splitFilePrefix.$splitFileExtension"
    shellCmdStatus =shellCmdRun(shellCmdRenameInputFile)
    val inputFilePath = s"$edgeNodeWritePath/s$splitFilePrefix.$splitFileExtension"
    val parts=splitNumberOfRecords
    //val splitCmd = s"""split --verbose -d -n l/${parts} --additional-suffix=.${fileFormat} --filter='([ $$FILE != "${splitFilePrefix}.00.${fileFormat}" ] && head -1 "${splitFilePrefix}.${fileFormat}" ; cat) > $$FILE' "${inputFilePath}" "${splitFilePrefix}." """
    val splitCmd = s"cd $edgeNodeWritePath;split -l ${parts}  $inputFilePath $splitFilePrefix"
    val shellCmdMoveFile = s"cd $edgeNodeSplitPath;rm -f $splitFilePrefix*;mv $edgeNodeWritePath/${splitFilePrefix}a*  $edgeNodeSplitPath"
    val shellCmdAddHeaderFile = s"sed -i '1i$header' $edgeNodeWritePath/${splitFilePrefix}*"
    val shellCmdRenameFile = s"cd $edgeNodeWritePath;find $edgeNodeSplitPath -type f -print | xargs -I % mv % %.$splitFileExtension"


    shellCmdStatus =shellCmdRun(splitCmd)
    shellCmdStatus =shellCmdRun(shellCmdMoveFile)
    shellCmdStatus =shellCmdRun(shellCmdAddHeaderFile)
    shellCmdStatus =shellCmdRun(shellCmdRenameFile)

    shellCmdStatus
  }

  /**
   * Will move files from one location to another without deleting from source
   *
   * @param listOfFilteredHdfsFileLocations  List of source files to be moved
   * @param inputEphemeralPath               Move location
   * @param zerobyteabort                    Defaulyted to false.If not set will not delete from listOfFilteredHdfsFileLocations

   * @return  Unit
   */

  def moveEphemeralFilesToProcess(
                                   listOfFilteredHdfsFileLocations:List[String],
                                   inputEphemeralPath:String,
                                   zerobyteabort:Boolean = false,

                                   deleteSource:Boolean=false)(implicit sparkSession: SparkSession): Unit = {
    for (hdfsFileLocations <- listOfFilteredHdfsFileLocations) {
      log.info(s"Will copy or move file $hdfsFileLocations to $inputEphemeralPath based on filter $deleteSource")
      val ephemeralMovedFilesize: BigInt = FileWriterService.hdfsMove(
        FileWriterService(hdfsFileLocations,
          inputEphemeralPath,
          zerobyteabort,
          deletesource = deleteSource))(sparkSession)
    }
  }

}
