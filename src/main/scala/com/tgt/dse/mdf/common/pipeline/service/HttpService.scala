package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.exceptions.HttpException
import scalaj.http.{Http, HttpOptions}

object HttpService extends LoggingTrait {


  def httpGet(httpUrl: String,
              headers: Seq[(String, String)],
              params: Seq[(String, String)] = Seq.empty,
              options: Seq[HttpOptions.HttpOption],
              option: HttpOptions.HttpOption) = {

    val yarnHttpResultBody: String = try {
      val yarnHttpResult = if (params.isEmpty) {
        Http(httpUrl)
          .options(options)
          .headers(headers)
          .option(option)
          .asString
      } else {
        Http(httpUrl)
          .options(options)
          .headers(headers)
          .option(option)
          .params(params)
          .asString
      }

      val yarnHttpResponse: String = if (yarnHttpResult.code == 200) {
        yarnHttpResult.body
      }
      else {
        throw HttpException("Bad HTTP response: code ="  + yarnHttpResult.code)
      }
      if (yarnHttpResponse == "null") log.info("No Response from api")
      log.info("==========httpResultBody==========")
      log.info(yarnHttpResponse)

      yarnHttpResponse
    }
    catch {
      case e: Exception => log.info("Error in Get request: " + e.getMessage)
        "ERROR"
    }
    yarnHttpResultBody
  }

}