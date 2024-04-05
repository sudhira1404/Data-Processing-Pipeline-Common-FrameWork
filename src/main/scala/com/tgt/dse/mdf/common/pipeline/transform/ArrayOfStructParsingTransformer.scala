package com.tgt.dse.mdf.common.pipeline.transform


import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, lit, udf}
import org.apache.spark.sql.{DataFrame, Row}

@SuppressWarnings(Array("all"))
object ArrayOfStructParsingTransformer {


  /** **
   *  Usage:
   *  input:
   *  val data = Seq(
   *  """{"actions": [{ "action_type": "link_click", "value": "5" },
   *  { "action_type": "page_engagement",   "value": "6"  },{ "action_type": "post_engagement", "value": "7 " }],
   *  "cost_per_action_type": [{ "action_type": "link_click", "value": "100" },
   *  { "action_type": "page_engagement",   "value": "101"  },
   *  { "action_type": "post_engagement", "value": "102 " }]}""".stripMargin
   *  )
   *  output:
   *  link_click_value  page_engagement_value post_engagement_value link_click_cost  page_engagement_cost post_engagement_cost
   *  5                         6                 7                   100               101                   102
   *  explodedColumnNames--should match with value present in the key structKeyColumnName1 in the arrayofstruct column
   *  val explodedColumnNames = Seq("link_click","page_engagement","post_engagement","post_reaction",
   *  "post_save","post","like","comment","onsite_conversion.post_save")
   *  sample call:
   *  val inputDf= spark.read.json(data.toDS())
   *  val valueexplodedDf = arrayOfStructParsing(inputDf,"actions","action_type","value",explodedColumnNames,"value")
   *  val costexplodedDf = arrayOfStructParsing(valueexplodedDf,"cost_per_action_type","action_type","value",explodedColumnNames,"cost")
   *
   *  or could be used like
   *
   *  inputDf.select(searchInArrayOfStruct(col("actions"),
   *  lit("link_click"),lit("action_type"),lit("value")).as("link_click"),
   *  searchInArrayOfStruct(col("actions"),
   *  lit("page_engagement"),lit("action_type"),lit("value")).as("page_engagement"),
   *  searchInArrayOfStruct(col("actions"),col("*"))
   */
  def arrayOfStructParsing(df: DataFrame, arrayOfStructColumnName: String, structKeyColumnName1: String,
                           structKeyColumnName2: String, explodedColumnNames: Seq[String]
                           , targetColumnSuffix: String): DataFrame = {
    val columns = df.columns.mkString(",")
    val seqColumns = columns.split(",").map(_.trim).toList
    val seqColumnsNew = seqColumns ++ explodedColumnNames
    val exists = (ValueToSearchInStructKeyColumnName1: String, seqColumns: Seq[String]) => {
      val existCheck: Boolean = seqColumns.contains(ValueToSearchInStructKeyColumnName1)
      existCheck
//      if (existCheck)
//        true
//      else
//        false
    }
    val explodedDf = df.select(seqColumnsNew.map(c => if (exists(c, seqColumns) ) col(c) else searchInArrayOfStruct(col(arrayOfStructColumnName),
      lit(c), lit(structKeyColumnName1), lit(structKeyColumnName2)).as(c.replaceAll("\\.", "_") + "_" + targetColumnSuffix)): _*)
    explodedDf
  }


  def searchInArrayOfStruct: UserDefinedFunction = udf { (inputArrayOfStruct: Seq[Row],
                                                          ValueToSearchInStructKeyColumnName1: String,
                                                          structKeyColumnName1: String, structKeyColumnName2: String) =>

    try {
      inputArrayOfStruct.find(_.getAs[String](structKeyColumnName1) == ValueToSearchInStructKeyColumnName1) match {
        case Some(i) => i.getAs[String](structKeyColumnName2)
        case None => null
      }
    }
    catch {
      case _: Exception => null
    }
  }

  def structParsing( df:DataFrame,arrayOfStructColumnName:Seq[String],
                     ValueToSearchInStructKeyColumnName1:String,structKeyColumnName1: String,
                     structKeyColumnName2: String
                     ,targetColumnSuffix:String): DataFrame = {

    val finalDf = arrayOfStructColumnName.foldLeft(df){
      case(df,colName) => df.withColumn(colName + "_" + targetColumnSuffix,searchInArrayOfStruct(col(colName),
        lit(ValueToSearchInStructKeyColumnName1),lit(structKeyColumnName1),lit(structKeyColumnName2)))
    }
    finalDf
  }

  /** *
   *  Usage:
   *  val data = Seq(
   *  """{"actions": [{ "action_type": "link_click", "value": "5" }, { "action_type": "page_engagement",   "value": "6"  },
   *  { "action_type": "post_engagement", "value": "7 " }],
   *  "cost_per_action_type": [{ "action_type": "link_click", "value": "100" },
   *  { "action_type": "page_engagement",   "value": "101"  },{ "action_type": "post_engagement", "value": "102 " }]}""".stripMargin
   *  )
   *
   *  output:
   *  link_click_value  page_engagement_value post_engagement_value link_click_cost  page_engagement_cost post_engagement_cost
   *  5                         6                 7                   100               101                   102
   *  sample call:
   *  import spark.implicits._
   *  val data = Seq(
   *  """{"actions": [{ "action_type": "link_click", "value": "5" },
   *  { "action_type": "page_engagement",   "value": "6"  },{ "action_type": "post_engagement", "value": "7 " }],
   *  "cost_per_action_type": [{ "action_type": "link_click", "value": "100" },
   *  { "action_type": "page_engagement",   "value": "101"  },{ "action_type": "post_engagement", "value": "102 " }]}""".stripMargin
   *  )
   *  val inputDf= spark.read.json(data.toDS())
   *  val expoldeColumns=Seq("actions","cost_per_action_type")
   *  //Call the explodeColumns function.Will explode the array or map type into rows
   *  val explodedDf =  explodeColumns(inputDf,expoldeColumns,"_exploded")
   *  //Access the struct values
   *  val parsedExpoldedDf =  explodedDf.select(col("actions_exploded.action_type").as("exploded_action_type"),
   *  col("actions_exploded.value").as("exploded_action_value"),
   *  col("cost_per_action_type_exploded.action_type").as("exploded_action_cost_type"),
   *  col("cost_per_action_type_exploded.value").as("exploded_action_cost_value"),
   *  col("*"))
   *  parsedExpoldedDf.drop("exploded_actions").drop("exploded_actions_cost").show(false)
   *  val FOUNDATIONINSERTCOLUMNS = Seq("actions","cost_per_action_type")
   *  val dropParsedExpoldedDf = parsedExpoldedDf.drop("actions_exploded").drop("cost_per_action_type_exploded")
   *  //Parse the values and group by to make it one record
   *  dropParsedExpoldedDf.groupBy(FOUNDATIONINSERTCOLUMNS.map(m=>col(m)):_*).agg(
   *  max(when('exploded_action_type === "link_click", 'exploded_action_value)).alias("link_click"),
   *  max(when('exploded_action_type === "page_engagement", 'exploded_action_value)).alias("page_engagement"),
   *  max(when('exploded_action_type === "post_engagement", 'exploded_action_value)).alias("post_engagement"),
   *  max(when('exploded_action_type === "post_reaction", 'exploded_action_value)).alias("post_reaction"),
   *  max(when('exploded_action_cost_type === "link_click", 'exploded_action_cost_value)).alias("link_click_cost"),
   *  max(when('exploded_action_cost_type === "page_engagement", 'exploded_action_cost_value)).alias("page_engagement_cost"),
   *  max(when('exploded_action_cost_type === "post_engagement", 'exploded_action_cost_value)).alias("post_engagement_cost")
   *  ).show(false)
   *
   */
  def explodeColumns(df: DataFrame, expoldecColumns: Seq[String], columnSufix: String): DataFrame = {
    val explodedDf = expoldecColumns.foldLeft(df)((df, column) => df.withColumn(column + columnSufix, explode(col(column))))
    explodedDf
  }

}
