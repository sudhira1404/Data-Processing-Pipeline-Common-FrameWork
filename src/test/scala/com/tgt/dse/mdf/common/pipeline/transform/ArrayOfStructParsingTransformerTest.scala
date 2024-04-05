package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.transform.ArrayOfStructParsingTransformer.{arrayOfStructParsing, explodeColumns, searchInArrayOfStruct}
import org.apache.spark.sql.functions.lit
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class ArrayOfStructParsingTransformerTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

    "The ApplicationUtilsTest object" should "do the following"

    it should "search in array of struct for the input value and get corresponding value in another struct key column " in {

      val data = Seq(
        """{"actions": [{ "action_type": "link_click", "value": "5" }, { "action_type": "page_engagement",   "value": "6"  },
           { "action_type": "post_engagement", "value": "7 " }],
           "cost_per_action_type": [{ "action_type": "link_click", "value": "100" },
       { "action_type": "page_engagement",   "value": "101"  },
       { "action_type": "post_engagement", "value": "102 " }]}""".stripMargin
      )

      val explodedColumnNames = Seq("link_click", "page_engagement", "post_engagement", "post_reaction","post_save",
        "post", "like", "comment", "onsite_conversion.post_save")
      import spark.implicits._
      val inputDf = spark.read.json(data.toDS())


      val valueexplodedDf = arrayOfStructParsing(inputDf, "actions", "action_type", "value", explodedColumnNames, "value")

      val linkClickValue = valueexplodedDf.select("link_click_value").collect().map(_.getString(0)).mkString("")

      assert(linkClickValue equals "5")
    }

    it should "search in array of struct for the input value and get corresponding value in another struct key column using select " in {

      val data = Seq(
        """{"actions": [{ "action_type": "link_click", "value": "5" },
            { "action_type": "page_engagement",   "value": "6"  },
            { "action_type": "post_engagement", "value": "7 " }],
       "cost_per_action_type": [{ "action_type": "link_click", "value": "100" },
        { "action_type": "page_engagement",   "value": "101"  },
        { "action_type": "post_engagement", "value": "102 " }]}""".stripMargin
      )

      import spark.implicits._
      val inputDf= spark.read.json(data.toDS())

      val linkClickValueDf = inputDf.select(searchInArrayOfStruct(inputDf("actions"),lit("link_click"), lit("action_type"),lit("value")))

      val linkClickValue = linkClickValueDf.select("UDF(actions, link_click, action_type, value)").collect().map(_.getString(0)).mkString("")

      assert(linkClickValue equals "5")

    }

  it should "explode array column into multiple rows" in {

    import spark.implicits._
    val data = Seq(
      """{"actions": [{ "action_type": "link_click", "value": "5" },
          { "action_type": "page_engagement",   "value": "6"  },{ "action_type": "post_engagement", "value": "7 " }],
          "cost_per_action_type": [{ "action_type": "link_click", "value": "100" },
           { "action_type": "page_engagement",   "value": "101"  },
           { "action_type": "post_engagement", "value": "102 " }]}""".stripMargin
    )
    val inputDf = spark.read.json(data.toDS())
    val expoldeColumns = Seq("actions", "cost_per_action_type")
    //Call the explodeColumns function.Will explode the array or map type into rows
    val explodedDf = explodeColumns(inputDf, expoldeColumns, "_exploded")
    val explodedCount = explodedDf.count()

    assert(explodedCount == 9)

  }
}
