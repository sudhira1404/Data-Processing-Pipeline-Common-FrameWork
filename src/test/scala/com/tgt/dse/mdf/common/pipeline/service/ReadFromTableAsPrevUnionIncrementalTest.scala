package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.exceptions.ReadFromTableAsPrevUnionIncrementalException
import com.tgt.dse.mdf.common.pipeline.service.ReadFromTableAsPrevUnionIncremental._
import com.tgt.dse.mdf.common.pipeline.types.SnapShotType
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.findMinDateFromDataFrameAndSubtractDays
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.withRetries
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers


class ReadFromTableAsPrevUnionIncrementalTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen{

  it should "Read from table as previous and get records in prev not in incremental and union with incremental" in {

    import spark.implicits._

    val incrementalRightDF = Seq(
      (1, "2018-04-15", "s"),
      (2, "2018-04-15", "t")
    ).toDF("id", "date", "value")

    val expectedDF = Seq(
      (1, "2018-04-15", "s"),
      (2, "2018-04-15", "t")
    ).toDF("id", "date", "value")

    val joinCondition = "id = right_id and date = right_date"

    val mergeDF =  withRetries(3,1 * 3000,"test")(readFromTableAsPrevJoinIncremental(ReadFromTableAsPrevUnionIncremental(
      testMarketingPipelineConfig.pipeLineConfig,
      incrementalRightDF,
      tempFNDReadLocation="",
      joinCondition,
      inputHiveQuery="",
      selectColumns = Seq.empty[String],
      fndPartitionDateColName="date"
    )))

    if (mergeDF.isFailure) {
      throw ReadFromTableAsPrevUnionIncrementalException(s"ReadFromTableAsPrevUnionIncrementalTest error...$mergeDF")
    }


    mergeDF.get.show(false)

    val expectedCount = mergeDF.get.except(expectedDF).count.toString

    assert(expectedCount matches "0")
  }


  it should "Read from table as previous and union incremental after removing dups based on key and create snapshot" in {

    import spark.implicits._

    val prevFndSnapShotDF = Seq(
      (0, "2022-07-31", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")

    val incrementalSnapShotDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:21:29.062"),
      (2, "2022-08-02", "2022-08-08T23:21:29.062"),
      (3, "2022-08-03", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")

    val expectedSnapShotDF = Seq(
      (0, "2022-07-31", "2022-08-08T22:20:29.062"),

      (0, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:21:29.062"),

      (0, "2022-08-02", "2022-08-08T22:20:29.062"),
      (1, "2022-08-02", "2022-08-08T22:21:29.062"),
      (2, "2022-08-02", "2022-08-08T23:21:29.062"),

      (0, "2022-08-03", "2022-08-08T22:20:29.062"),
      (1, "2022-08-03", "2022-08-08T22:21:29.062"),
      (2, "2022-08-03", "2022-08-08T23:21:29.062"),
      (3, "2022-08-03", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")


    val mergeSnapShotDF = joinPrevIncrementalToDataFrames(prevFndSnapShotDF,
      incrementalSnapShotDF,
      joinCondition = "",
      fndKeyColName = Seq("id", "date"),
      fndOrderByColName = "timestamp")

    mergeSnapShotDF.show(false)

    val snapShotDF = generateSnapShot(mergeSnapShotDF,
      partitionColName = "date",
      keyColName = Seq("id", "date"),
      orderByColName = "timestamp",
      handleMissingDates = true,
      debugDFToLog = true)

    val snapshotSelectDF = snapShotDF.select("id", "date","timestamp")

    snapshotSelectDF.select("id", "date","timestamp").orderBy("date").show(false)

    val expectedCount = snapshotSelectDF.except(expectedSnapShotDF).count.toString

    assert(expectedCount matches "0")
  }


  it should "Read from table as previous and union incremental after removing dups based on key and create snapshot with missing dates" in {

    import spark.implicits._

    val prevFndMissingSnapShotDF = Seq(
      (0, "2022-07-31", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")

    val incrementalMissingSnapShotDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (2, "2022-08-03", "2022-08-08T23:21:29.062"),
      (3, "2022-08-05", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")

    val expectedMissingSnapShotDF = Seq(
      (0, "2022-07-31", "2022-08-08T22:20:29.062"),

      (0, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),

      (0, "2022-08-02", "2022-08-08T22:20:29.062"),
      (1, "2022-08-02", "2022-08-08T22:20:29.062"),

      (0, "2022-08-03", "2022-08-08T22:20:29.062"),
      (1, "2022-08-03", "2022-08-08T22:20:29.062"),
      (2, "2022-08-03", "2022-08-08T23:21:29.062"),

      (0, "2022-08-04", "2022-08-08T22:20:29.062"),
      (1, "2022-08-04", "2022-08-08T22:20:29.062"),
      (2, "2022-08-04", "2022-08-08T23:21:29.062"),

      (0, "2022-08-05", "2022-08-08T22:20:29.062"),
      (1, "2022-08-05", "2022-08-08T22:20:29.062"),
      (2, "2022-08-05", "2022-08-08T23:21:29.062"),
      (3, "2022-08-05", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")


    val mergeMissingSnapShotDF = joinPrevIncrementalToDataFrames(prevFndMissingSnapShotDF,
      incrementalMissingSnapShotDF,
      joinCondition = "",
      fndKeyColName = Seq("id", "date"),
      fndOrderByColName = "timestamp")

    mergeMissingSnapShotDF.show(false)

    val missingSnapShotDF = generateSnapShot(mergeMissingSnapShotDF,
      partitionColName = "date",
      keyColName = Seq("id", "date"),
      orderByColName = "timestamp",
      handleMissingDates = true,
      debugDFToLog = true)


    val snapshotSelectDF = missingSnapShotDF.select("id", "date","timestamp")

    snapshotSelectDF.select("id", "date","timestamp").orderBy("date").show(false)

    val expectedCount = snapshotSelectDF.except(expectedMissingSnapShotDF).count.toString

    assert(expectedCount matches "0")
  }

  it should "Read from table as previous join with incremental and get  records from FND table" +
    " not in incremental and union with incremental and remove dups" in {

    import spark.implicits._

    val PrevDF = Seq(
      (0, "2022-08-01", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")

    val incrementalDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:21:29.062"),
      (2, "2022-08-02", "2022-08-08T23:21:29.062"),
      (3, "2022-08-03", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")


    val expectedDF = Seq(
      (0, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:21:29.062"),
      (2, "2022-08-02", "2022-08-08T23:21:29.062"),
      (3, "2022-08-03", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")

    val joinCondition = "id = right_id and date = right_date"
    val mergeDF = joinPrevIncrementalToDataFrames(PrevDF,
      incrementalDF,
      joinCondition
      ,fndKeyColName=Seq("id","date"),
      fndOrderByColName="timestamp"
    )

    mergeDF.select("id", "date","timestamp").orderBy("date").show(false)
    val expectedCount = mergeDF.except(expectedDF).count.toString

    assert(expectedCount matches "0")
  }

  it should "Read from table as previous union with incremental and remove dups based on keys"  in {

    import spark.implicits._

    val PrevNoJoinDF = Seq(
      (0, "2022-08-01", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")

    val incrementalNoJoinDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:21:29.062"),
      (2, "2022-08-02", "2022-08-08T23:21:29.062"),
      (3, "2022-08-03", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")


    val expectedNoJoinDF = Seq(
      (0, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:21:29.062"),
      (2, "2022-08-02", "2022-08-08T23:21:29.062"),
      (3, "2022-08-03", "2022-08-08T23:21:29.062")
    ).toDF("id", "date", "timestamp")


    val mergeNoJoinDF = joinPrevIncrementalToDataFrames(PrevNoJoinDF,
      incrementalNoJoinDF,
      joinCondition="",
      fndKeyColName=Seq("id","date"),
      fndOrderByColName="timestamp")

    val mergeNoJoinSelectDF = mergeNoJoinDF.select("id", "date","timestamp").orderBy("date")

    mergeNoJoinDF.select("id", "date","timestamp").orderBy("date").show(false)

    val expectedCount = mergeNoJoinSelectDF.except(expectedNoJoinDF).count.toString

    assert(expectedCount matches "0")
  }

  it should "Should iterate over date column in the inputDF to get distinct list of dates  and " +
    "get min data and subtracts from daysToSubtract parameter" in {

    import spark.implicits._
    val sourceDF = Seq(
      ("2022-08-10", "2022-08-10"),
      ("2022-08-11", "2022-08-11"),
      ("2022-08-12", "2022-08-12"),
      ("2022-08-13", "2022-08-13")
    ).toDF("report_d", "file_date")


    val actualResult = findMinDateFromDataFrameAndSubtractDays(
      sourceDF,
      "report_d",
      "yyyy-MM-dd",
      1)

    assert(actualResult matches "2022-08-09")


  }


  it should "Build snapshot"  in {

    import spark.implicits._

    val incrementalDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (2, "2022-08-02", "2022-08-08T22:20:29.062"),
      (3, "2022-08-03", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")


    val expectedDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-02", "2022-08-08T22:20:29.062"),
      (2, "2022-08-02", "2022-08-08T22:20:29.062"),
      (1, "2022-08-03", "2022-08-08T22:20:29.062") ,
      (2, "2022-08-03", "2022-08-08T22:20:29.062") ,
      (3, "2022-08-03", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")


    val mergeDF = SnapShotType.buildSnapShot(
      testConfig,
      incrementalDF,
      selectColumns=Seq("id", "date", "timestamp"),
      fndPartitionDateColName = "date",
      fndKeyColName = Seq("id", "date", "timestamp"),
      fndOrderByColName = "timestamp",
      snapShot_F = true,
      snapShotFndKeyColName = Seq("id", "date", "timestamp"),
      snapShotFndOrderByColName = "timestamp",
      snapShotWindowingStrategy = "row_number",
      snapShotHandleMissingDates = true,
      snapShotDebugDFToLog = false,
      fndSelectColumns = "id, date, timestamp",
      tableIsExternal = true
    )


    val mergeOrderByDF = mergeDF.select("id", "date","timestamp").orderBy("date")

    mergeOrderByDF.select("id", "date","timestamp").orderBy("date").show(false)

    val expectedCount = mergeOrderByDF.except(expectedDF).count.toString

    assert(expectedCount matches "0")
  }

  it should "Load from fnd After Merging Fnd With Incremental"  in {

    import spark.implicits._

    val incrementalDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (2, "2022-08-02", "2022-08-08T22:20:29.062"),
      (3, "2022-08-03", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")


    val expectedDF = Seq(
      (1, "2022-08-01", "2022-08-08T22:20:29.062"),
      (2, "2022-08-02", "2022-08-08T22:20:29.062"),
      (3, "2022-08-03", "2022-08-08T22:20:29.062")
    ).toDF("id", "date", "timestamp")


    val mergeFndDF = SnapShotType.LoadFndAfterMergingFndWithIncremental(
      testConfig,
      incrementalDF,
      selectColumns=Seq("id", "date", "timestamp"),
      fndPartitionDateColName = "date",
      fndKeyColName = Seq("id", "date", "timestamp"),
      fndOrderByColName = "timestamp"
    )

    val mergeOrderByDF = mergeFndDF.select("id", "date","timestamp").orderBy("date")

    mergeOrderByDF.select("id", "date","timestamp").orderBy("date").show(false)

    val expectedCount = mergeOrderByDF.except(expectedDF).count.toString

    assert(expectedCount matches "0")

  }

}
