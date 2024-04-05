package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.exceptions.NoSqlException
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.util.Properties
import java.util.regex.Pattern
import javax.mail._
import javax.mail.internet._

/**
 * EmailService Object
 * Members
 * fromAddres:                From Address,
 * toAddress:                 To Address,
 * reportFilename:            Report Filename attached to the email,
 * emailSubject:              Email Subject line,
 * emailBodySubject:          Subject line in the message body,
 * attachDFResult:            Boolean.If set then will attach DF to the email,
 * attachDFResultToEmailBody: Boolean.If set then will attach DF to the email body
 * debug:                     String."true" or "false.If set then will Debug mode for email service is turned on
 * customEmailBody:           Defaulted to empty string.If provided ,will be used in the email body.
 *                            If attachDFResultToEmailBody is set then this will take precedence over customEmailBody
 */
case class EmailService(
                         fromAddress: String,
                         toAddress: String,
                         reportFilename:String="",
                         emailSubject:String,
                         emailBodySubject:String,
                         attachDFResult:Boolean,
                         attachDFResultToEmailBody:Boolean,
                         debug:String,
                         customEmailBody:String="")


object EmailService extends LoggingTrait {

  /**
   * This method is to send an email to the given address by attaching the input dataframe.
   * If attachDFResult and attachDFResultToEmailBody is set to false then DF will not be attached but still
   * needs to provide a dummy dataframe as inputDF
   * @param inputDF                   Dataframe to be attached in email
   * @param fromAddress               From Address
   * @param toAddress                 To Address
   * @param reportFilename            Defaulted to empty string.Report Filename attached to the email
   * @param emailSubject              Email Subject line
   * @param emailBodySubject          Subject line in the message body
   * @param attachDFResult            Boolean.If set then will attach DF as a file to the email
   * @param attachDFResultToEmailBody Boolean.If set then will attach DF to the email body in html format.This take precedence over customEmailBody parameter if both are set.
   * @param debug                     String.Defaulted to "false".If set to "true" then debug mode is turned on for email service
   * @param customEmailBody           Defaulted to empty string.If provided ,will be used in the email body.If attachDFResultToEmailBody is set then this will take precedence over customEmailBody
   * @return - Unit
   */
  def sendEmail(inputDF : DataFrame,
                fromAddress: String,
                toAddress: String,
                reportFilename:String="",
                emailSubject:String,
                emailBodySubject:String,
                attachDFResult:Boolean,
                attachDFResultToEmailBody:Boolean,
                debug:String="false",
                customEmailBody:String=""
               ) {

    val properties = new Properties();
    properties.setProperty("mail.smtps.host", "smtp.target.com")
    properties.setProperty("mail.smtp.port", "25")
    properties.put("mail.smtp.auth", "false")
    properties.put("mail.debug", debug)
    properties.put("mail.transport.protocol", "smtp")
    properties.put("mail.smtp.starttls.enable", "false")

    val session = Session.getDefaultInstance(properties)

    val multipart = new MimeMultipart()
    val messageBodyPart = new MimeBodyPart()
    val messageBodyPartText = new MimeBodyPart()
    messageBodyPartText.setText(emailBodySubject)

    if (attachDFResult) {
      dataFrameToFile(inputDF, reportFilename)
      messageBodyPart.attachFile(reportFilename)
      val filename = reportFilename
      messageBodyPart.setFileName(filename)
      multipart.addBodyPart(messageBodyPart)
    }

    if (attachDFResultToEmailBody) {
      val msg = createHtmlEmailBody(inputDF, emailBodySubject)
      messageBodyPartText.setContent(
        msg,
        "text/html")
      multipart.addBodyPart(messageBodyPartText)
    }

    if (customEmailBody != "") {
      messageBodyPartText.setContent(
        customEmailBody,
        "text/html")
      multipart.addBodyPart(messageBodyPartText)
    }
    else
    {
      messageBodyPart.setText(emailBodySubject)
      multipart.addBodyPart(messageBodyPartText)
    }

    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(fromAddress))
    message.setRecipients(Message.RecipientType.TO, toAddress)
    message.setHeader("Content-Type", "text/html")
    message.setSubject(emailSubject)
    //if (attachDFResult || attachDFResultToEmailBody) {
    message.setContent(multipart)
    //}

    val transport = session.getTransport("smtp")
    transport.connect()
    Transport.send(message)
    transport.close()
  }

  /**
   * Will create a html email body based on the columns in the dataframe
   * @param df                      input df
   * @param emailBodySubject        emailBodySubject
   * @return Unit
   */
  def createHtmlEmailBody(df: DataFrame,emailBodySubject:String): String = {
    val columnNames = df.columns.map(x => "<th>" + x.trim + "</th>").mkString
    val data = df.collect.mkString
    val data1 = data.split(",").map(x => "<td>".concat(x).concat("</td>"))
    val data2 = data1.mkString.replaceAll("<td>\\[", "<tr><td>")
    val data3 = data2.mkString.replaceAll("]\\[", "</td></tr><td>").replaceAll("]", "")

    val msg =
      s"""<!DOCTYPE html>
         |<html>
         |   <head>
         |      <style>
         |         table {
         |            border: 1px solid black;
         |         }
         |         th {
         |          border: 1px solid black;
         |          background-color: #FFA;
         |          }
         |         td {
         |          border: 1px solid black;
         |          background-color: #FFF;
         |          }
         |      </style>
         |   </head>
         |
         |   <body>
         |      <h1>$emailBodySubject</h1>
         |      <table>
         |         <tr> $columnNames </tr> $data3
         |      </table>
         |   </body>
         |</html>""".stripMargin

    msg
  }

  /**
   * Will print to a file
   * @param f                      java.io.File
   * @param op                     java.io.PrintWriter
   * @return Unit
   */
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  /**
   * Dataframe will be saved to a file
   * @param inputDF                      input df
   * @param filename                     filename
   * @return Unit
   */
  def dataFrameToFile(inputDF:DataFrame,filename:String) {

    val header = inputDF.schema.fieldNames.toSeq
    val f = new File(filename)
    printToFile(f) {
      p =>
        header.foreach(e => {
          p.print(e)
          p.print(",")
        })
        p.print(",")
        p.print("\n")
        inputDF.collect().foreach(row => {
          row.toSeq.foreach(e => {
            p.print(e)
            p.print(",")
          })
          p.print("\n")
        })
    }
  }

  /**
   * Wrapper to send email attaching dataframe result
   * @param inputDF                       input df
   * @param sendEmailFlag                 Defaulted to true.If send will send email.
   * @param sendEmailAggregateInputDFFlag Boolean.If set to true will use  sendEmailAggregateSql to aggregate  inputDF which will be send in the email
   * @param sendEmailAggregateSql         Defaulted to empty string.Aggregated sql executed against the inputDF.
   *                                      Need to be provided if sendEmailAggregateInputDFFlag is set
   * @param sendEmailService              EmailService object
   * @return Unit
   */
  def sendEmailWrapper(inputDF: DataFrame,
                       sendEmailFlag: Boolean=true,
                       sendEmailAggregateInputDFFlag: Boolean,
                       sendEmailAggregateSql: String="",
                       sendEmailService: EmailService)
                      (implicit sparkSession: SparkSession): Unit = {
    if (sendEmailFlag) {
      if (sendEmailAggregateInputDFFlag) {
        inputDF.createOrReplaceTempView("inputDF")
        if (sendEmailAggregateSql.isEmpty) throw NoSqlException("Provide sql to aggregate the input DF")
        val pattern = "(FROM|from)\\s+(?<table>\\S+)"
        val compile = Pattern.compile(pattern)
        val matcher = compile.matcher(sendEmailAggregateSql)
        var tableName = ""
        val value: Unit = while (matcher.find) {
          tableName = matcher.group("table")
        }
        val newSqlStatement = sendEmailAggregateSql.replace(tableName, "inputDF")
        log.info(s"Send email aggregated/custom sql statement that will executed==>$newSqlStatement")
        sendEmail(sparkSession.sql(newSqlStatement),
          sendEmailService.fromAddress,
          sendEmailService.toAddress,
          sendEmailService.reportFilename,
          sendEmailService.emailSubject,
          sendEmailService.emailBodySubject,
          sendEmailService.attachDFResult,
          sendEmailService.attachDFResultToEmailBody,
          sendEmailService.debug,
          sendEmailService.customEmailBody
        )
      }
      else {
        sendEmail(inputDF,
          sendEmailService.fromAddress,
          sendEmailService.toAddress,
          sendEmailService.reportFilename,
          sendEmailService.emailSubject,
          sendEmailService.emailBodySubject,
          sendEmailService.attachDFResult,
          sendEmailService.attachDFResultToEmailBody,
          sendEmailService.debug,
          sendEmailService.customEmailBody
        )
      }

    }
  }

}
