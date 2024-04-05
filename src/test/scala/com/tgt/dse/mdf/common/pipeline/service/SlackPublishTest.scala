package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import org.scalatest.flatspec.AnyFlatSpec

class SlackPublishTest extends AnyFlatSpec with MarketingTestHelpers {

  it should "Post a success message to Slack" in {
    val TEST_MESSAGE: String = "This is a test of the unit variety. This is only a test."
    val SLACK_ID: String = "dStqc8EeBLuKxRu5hdV4I0gWk0V4TPBnQ108UL2rwlEy7Xm4enCar6xMJ9Jp4IPn"
    SlackPublish.sendSlackMessage(TEST_MESSAGE,SLACK_ID)
  }

  it should "Post a success message to Slack that has double quotes in the message" in {
    val TEST_MESSAGE: String = "This is a test of the unit variety. This tests having \"double quotes\" inside the message"
    val SLACK_ID: String = "dStqc8EeBLuKxRu5hdV4I0gWk0V4TPBnQ108UL2rwlEy7Xm4enCar6xMJ9Jp4IPn"
    SlackPublish.sendSlackMessage(TEST_MESSAGE,SLACK_ID)
  }
}
