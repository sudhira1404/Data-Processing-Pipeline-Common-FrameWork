package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.exceptions.ConfigException
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants

import java.io._
import java.net.{HttpURLConnection, URL}
import scala.util.{Failure, Success, Try}


object SlackPublish extends LoggingTrait {

  /**
   * Send message to slack channel for which the webhook url been passed
   * Process of configuring webhook url is as below
   * 1. Install the SpiderBot app https://confluence.target.com/display/SLACK/Spiderbot in the slack channel where message needs to be sent
   * 2. /webhook create, based on parameters webhook will be created
   * 3. You will get the webhook url which needs to be kept as secret in the channel you have selected while creating webhook
   * 4. Test the token by curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' https://api.target.com/slack_events/v1/webhooks/[token]
   *
   * @param message message to be sent to slack
   * @param slackToken token recieved by SpiderBot
   * @return Status of message push
   */
  def sendSlackMessage(message :String , slackToken:String): Boolean ={
    Try {
      val payload: String = formatPayload(message)
      //Get connection
      val slackConnection: HttpURLConnection = openConnection(slackToken, MarketingConstants.WEBHOOK_URL)
      //Connect and send payload
      writeToUrl(slackConnection, payload)
      //Fetch response
      log.info(readFromUrl(slackConnection))
    } match {
      case Success(response) =>
        logInfo(s"Response from slack is $response")
        true
      case Failure(throwable) =>
        logError(s"Error in pushing message ${throwable.getMessage}")
        false
    }
  }

  private def formatPayload(message: String): String = {
    val escapeQuotes: String = message.replaceAll("\"", "\\\\\"")
    val payload = MarketingConstants.OPEN_BRACES
      .concat(MarketingConstants.TEXT_WITHQUOTES)
      .concat(MarketingConstants.COLON)
      .concat(MarketingConstants.QUOTES)
      .concat(escapeQuotes)
      .concat(MarketingConstants.QUOTES)
      .concat(MarketingConstants.CLOSE_BRACES)
    logInfo(s"payload to be  pushed is $payload")
    payload
  }

  private def openConnection(slackToken:String , webhookURL:String): HttpURLConnection = {
    Try {
      val url : URL = new URL( webhookURL + slackToken )
      val connection : HttpURLConnection = url
        .openConnection()
        .asInstanceOf[HttpURLConnection]
      connection.setDoOutput(true)
      connection.setRequestMethod(MarketingConstants.POST)
      connection.setRequestProperty(MarketingConstants.CONTENT_TYPE, MarketingConstants.APPLICATION_JSON)
      connection
    }match {
      case Success(httpURLConnection) =>
        logDebug("Able to do connection")
        httpURLConnection
      case Failure(throwable) =>
        logError("Could not connect to slack")
        throw ConfigException(s"Please validate config ${throwable.getMessage}")
    }
  }

  private def writeToUrl(slackConnection: HttpURLConnection, payload: String): Unit = {
    val outputStream :OutputStream  = slackConnection.getOutputStream
    val outputStreamWriter: OutputStreamWriter = new OutputStreamWriter(outputStream)
    //val bufferedWriter: BufferedWriter = new BufferedWriter(outputStreamWriter)
    try {
      outputStreamWriter.write(payload)
    } catch {
      case e: IOException => log.error(s"Exception writing to Slack: $e")
    } finally {
      //bufferedWriter.flush()
      //bufferedWriter.close()
      outputStreamWriter.flush()
      outputStreamWriter.close()
      outputStream.flush()
      outputStream.close()
    }
  }

  private def readFromUrl(slackConnection: HttpURLConnection): String = {
    val inputStream: InputStream = slackConnection.getInputStream
    val inputStreamReader: InputStreamReader = new InputStreamReader(inputStream)
    val bufferedReader: BufferedReader = new BufferedReader(inputStreamReader)

    val response: String = {
      try {
        Stream
          .continually(bufferedReader.readLine())
          .takeWhile(_ != null)
          .mkString("\n")
      } catch {
        case e: IOException =>
          log.error(s"Exception reading the response from Slack: $e")
          e.getMessage
      } finally {
        bufferedReader.close()
        inputStreamReader.close()
        inputStream.close()
        slackConnection.disconnect()
      }
    }
    response
  }

}