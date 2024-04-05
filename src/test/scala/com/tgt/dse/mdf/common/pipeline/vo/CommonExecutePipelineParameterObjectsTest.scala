package com.tgt.dse.mdf.common.pipeline.vo

import com.tgt.dse.mdf.common.pipeline.CommonExecutePipeLineHelpers
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects.{AddDatePartitionColumn, AddFileDate, ArchiveArchivePartition, ArchiveEphemeral, AtomicHistLoad, CommonPipeLineHelper, ConvertDateFormat, ConvertDatePartOfTsToDefault, ConvertMicroToCurrency, ConvertTimestampFromLocalToUtc, ConvertTimestampFromUtcToLocal, ConvertTimestampToDate, CountByFileDateAndPartitionDate, DeleteTriggerFile, DqCheck, DropDups, ExecuteExternalSql, ExecutionOrder, FndLoad, GetLatestByKeyCol, JoinCal, ReadInput, RemoveDollarCharacter, RemoveNonUtfCharacter, Rename, ReplaceNullWithDefaultForAmtColumn, ReplaceNullWithDefaultForQtyColumn, ReplaceNullWithDefaultForStringColumn, ShowCustomResultsToLog, ShowResultsToLog, ValidateDataType, ValidateDateFormat, ValidateNotNull, ValidateSchema}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.util.Try

class CommonExecutePipelineParameterObjectsTest  extends AnyFlatSpec with CommonExecutePipeLineHelpers with Matchers with GivenWhenThen{


  it should "validate ValidateSchema case class " in {

    val initializeValidateSchema = ValidateSchema(testConfig)

    assert(initializeValidateSchema.csvSchemaString matches "Source string, CompanyName string,AccountId string, CampaignName string, CampaignId string,CampaignBudget string, CampaignStartDate string, CampaignEndDate string, ClassId string, DepartmentId string,VendorNumber string, TCIN string, CalendarYear string, CalendarMonth string, Revenue string,filename string,CampaignJoinDate string,CampaignJoinDateTs string,CampaignJoinDateLocalTs string,CampaignJoinDateUtclTs string" )

    assert(initializeValidateSchema.templateLocation matches "")

  }

  it should "validate AddFileDate case class " in {

    val initializeAddFileDate = AddFileDate(testConfig)

    assert(initializeAddFileDate.sourceDateFormat matches "yyyy-MM-dd" )

    assert(initializeAddFileDate.sourceColumn matches "filename")

    assert(initializeAddFileDate.targetColumn matches "file_date")

  }

  it should "validate AddDatePartitionColumn case class " in {

    val initializeAddDatePartitionColumn = AddDatePartitionColumn(testConfig)

    assert(initializeAddDatePartitionColumn.isPartitionColumnDatatypeDate.toString matches "true" )

    assert(initializeAddDatePartitionColumn.sourceColumn matches "file_date")

    assert(initializeAddDatePartitionColumn.targetColumn matches "report_d")

  }

  it should "validate getLatestByKeyCol case class " in {

    val initializeGetLatestByKeyCol = GetLatestByKeyCol(testConfig)

    assert(initializeGetLatestByKeyCol.keyColName.map(_.trim).mkString(",") matches "report_d" )
    assert(initializeGetLatestByKeyCol.orderByColName matches "file_date")
    assert(initializeGetLatestByKeyCol.windowingStrategy matches "row_number")
  }


  it should "validate CountByFileDateAndPartitionDate case class " in {

    val initializeCountByFileDateAndPartitionDate = CountByFileDateAndPartitionDate(testConfig)

    assert(initializeCountByFileDateAndPartitionDate.partitionDateColumn.toString matches "report_d" )

    assert(initializeCountByFileDateAndPartitionDate.fileDateColumn matches "file_date")

  }

  it should "validate ExecuteExternalSql case class " in {

    val initializeExecuteExternalSql = ExecuteExternalSql(testConfig)

    assert(initializeExecuteExternalSql.dataFrameNameUsedInSql.map(_.trim).mkString(",") matches "replaceNullWithDefaultForAmtColumn,addFileDate" )

  }

  it should "validate DropDups case class " in {

    val initializeDropDups = DropDups(testConfig)

    assert(initializeDropDups.location matches "src/test/data/landing/citrus_invoice_revenue/data_processing_pipeline/output/dqcorruptrecords/" )

    assert(initializeDropDups.partitionColumns.map(_.trim).mkString(",") matches "Source,CompanyName,AccountId,CampaignName,CampaignId,CampaignStartDate,CampaignEndDate,ClassId,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth" )

  }


  it should "validate ValidateNotNull case class " in {

    val initializeValidateNotNull = ValidateNotNull(testConfig)

    assert(initializeValidateNotNull.inputColumns.map(_.trim).mkString(",") matches "Source,CompanyName,AccountId,CampaignName,CampaignId,CampaignStartDate,CampaignEndDate,ClassId,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth" )

  }

//  it should "validate ValidateDataType case class " in {
//
//    val initializeValidateDataType = ValidateDataType(testConfig)
//    assert(initializeValidateDataType.inputColumns.toString() == "Vector((CampaignBudget,decimal(38,2)), (Revenue,decimal(38,8)))")
//  }


  it should "validate ValidateDateFormat case class " in {

    val initializeValidateDateFormat = ValidateDateFormat(testConfig)

    assert(initializeValidateDateFormat.inputColumns.map(_.trim).mkString(",") matches "CampaignStartDate,CampaignEndDate" )
    assert(initializeValidateDateFormat.sourceDateFormat matches "yyyy-MM-dd" )

  }

  it should "validate RemoveDollarCharacter case class " in {

    val initializeRemoveDollarCharacter = RemoveDollarCharacter(testConfig)

    assert(initializeRemoveDollarCharacter.inputColumns.map(_.trim).mkString(",") matches "CampaignBudget,Revenue" )
  }

  it should "validate ReplaceNullWithDefaultForAmtColumn case class " in {

    val initializeReplaceNullWithDefaultForAmtColumn = ReplaceNullWithDefaultForAmtColumn(testConfig)

    assert(initializeReplaceNullWithDefaultForAmtColumn.inputColumns.map(_.trim).mkString(",") matches "CampaignBudget,Revenue" )
    assert(initializeReplaceNullWithDefaultForAmtColumn.default.toString matches "0.00" )

  }

  it should "validate ReplaceNullWithDefaultForQtyColumn case class " in {

    val initializeReplaceNullWithDefaultForQtyColumn = ReplaceNullWithDefaultForQtyColumn(testConfig)

    assert(initializeReplaceNullWithDefaultForQtyColumn.inputColumns.map(_.trim).mkString(",") matches "New_Customers,Unique_Customers,Impressions,Clicks,Sales,Leads,PotentialActions" )
    assert(initializeReplaceNullWithDefaultForQtyColumn.default.toString matches "0" )

  }

  it should "validate RemoveNonUtfCharacter case class " in {

    val initializeRemoveNonUtfCharacter = RemoveNonUtfCharacter(testConfig)

    assert(initializeRemoveNonUtfCharacter.inputColumns.map(_.trim).mkString(",") matches "" )
  }


  it should "validate ConvertMicroToCurrency case class " in {

    val initializeConvertMicroToCurrency = ConvertMicroToCurrency(testConfig)

    assert(initializeConvertMicroToCurrency.inputColumns.map(_.trim).mkString(",") matches "CampaignBudget,Revenue" )
  }

//  it should "validate HandleNullsInPKColumns case class " in {
//
//    val initializeHandleNullsInPKColumns = HandleNullsInPKColumns(testConfig)
//    assert(initializeHandleNullsInPKColumns.inputColumns.toString() == "Vector((CampaignId,123), (CompanyName,NA), (CampaignStartDate,9999-12-31))")
//
//  }

  it should "validate ConvertDatePartOfTsToDefault case class " in {

    val initializeConvertDatePartOfTsToDefault = ConvertDatePartOfTsToDefault(testConfig)

    assert(initializeConvertDatePartOfTsToDefault.inputColumns.map(_.trim).mkString(",") matches "CampaignEndDateTs" )
    assert(initializeConvertDatePartOfTsToDefault.inputTsFormat == "MM/dd/yy HH:mm")

  }

  it should "validate ConvertDateFormat case class " in {

    val initializeConvertDateFormat = ConvertDateFormat(testConfig)

    assert(initializeConvertDateFormat.inputColumns.map(_.trim).mkString(",") matches "CampaignJoinDate" )
    assert(initializeConvertDateFormat.inputDateFormat == "d/MM/yyyy")
    assert(initializeConvertDateFormat.outputDateFormat == "yyyy-MM-dd")

  }

  it should "validate ConvertTimestampToDate case class " in {

    val initializeConvertTimestampToDate = ConvertTimestampToDate(testConfig)

    assert(initializeConvertTimestampToDate.inputColumns.map(_.trim).mkString(",") matches "CampaignJoinDateTs" )
    assert(initializeConvertTimestampToDate.outputFormat == "yyyy-MM-dd")

  }

  it should "validate ConvertTimestampFromLocalToUtc case class " in {

    val initializeConvertTimestampFromLocalToUtc = ConvertTimestampFromLocalToUtc(testConfig)

    assert(initializeConvertTimestampFromLocalToUtc.inputColumns.map(_.trim).mkString(",") matches "CampaignJoinDateLocalTs" )
    assert(initializeConvertTimestampFromLocalToUtc.inputFormat == "yyyy-MM-dd'T'HH:mm:ss.SSS")
    assert(initializeConvertTimestampFromLocalToUtc.outputFormat == "yyyy-MM-dd HH:mm:ss")
    assert(initializeConvertTimestampFromLocalToUtc.timeZone == "America/Chicago")
  }


  it should "validate ConvertTimestampFromUtcToLocal case class " in {

    val initializeConvertTimestampFromUtcToLocal = ConvertTimestampFromUtcToLocal(testConfig)

    assert(initializeConvertTimestampFromUtcToLocal.inputColumns.map(_.trim).mkString(",") matches "CampaignJoinDateUtclTs" )
    assert(initializeConvertTimestampFromUtcToLocal.inputFormat == "yyyy-MM-dd'T'HH:mm:ss.SSS")
    assert(initializeConvertTimestampFromUtcToLocal.outputFormat == "yyyy-MM-dd HH:mm:ss")
    assert(initializeConvertTimestampFromUtcToLocal.timeZone == "America/Chicago")
  }



  it should "validate JoinCal case class " in {

    val initializeJoinCal = JoinCal(testConfig)

    assert(initializeJoinCal.calendarColumns == "calendar_d,day_n,fiscal_week_n,fiscal_week_end_d,fiscal_week_begin_d,calendar_fiscal_week,fiscal_month_id,fiscal_month_abbreviation_n,fiscal_month_n , fiscal_quarter_id , fiscal_quarter_n, fiscal_year_id ,fiscal_year_week_id")
    assert(initializeJoinCal.calendarColumnsWithAnyTransformation == "calendar_d,day_n,fiscal_week_n,fiscal_week_end_d,fiscal_week_begin_d,concat(fiscal_week_begin_d , '-' ,fiscal_week_end_d) calendar_fiscal_week,fiscal_month_id,fiscal_month_abbreviation_n  fiscal_month_n , fiscal_quarter_id , fiscal_quarter_n, fiscal_year_id ,fiscal_year_week_id")
    assert(initializeJoinCal.calendarTable == "prd_cal_fnd.calendar")
    assert(initializeJoinCal.tempCalendarExtLocation == "/user/SVMDEDMD/hive/citrus_invoice_revenue/calendar")
    assert(initializeJoinCal.inputJoinConditionColumn == "report_d")
    assert(initializeJoinCal.calendarJoinConditionColumn == "calendar_d")
  }

//  it should "validate Rename case class " in {
//
//    val initializeRename = Rename(testConfig)
//    println(initializeRename.renameColumns.toString())
//    assert(initializeRename.renameColumns.toString() == "Vector((CompanyName,company_name), (DepartmentId,dept_id), (CampaignStartDate,campaign_start_date), (CampaignEndDate,campaign_end_date), (ClassId,class_id), (CampaignName,campaign_name), (CalendarYear,calendar_year), (Revenue,revenue_a), (CampaignBudget,campaign_budget_a), (Source,source_name), (AccountId,account_id), (VendorNumber,vendor_number), (CalendarMonth,calendar_month), (TCIN,tcin), (CampaignId,campaign_id))")
//
//  }

  it should "validate ArchiveArchivePartition case class " in {

    val initializeArchiveArchivePartition = ArchiveArchivePartition(testConfig)

    assert(initializeArchiveArchivePartition.deleteSource.toString == "false")
  }

  it should "validate ArchiveEphemeral case class " in {

    val initializeArchiveEphemeral = ArchiveEphemeral(testConfig)

    assert(initializeArchiveEphemeral.deleteSource.toString == "true")
  }

  it should "validate AtomicHistLoad case class " in {

    val initializeAtomicHistLoad = AtomicHistLoad(testConfig)

    assert(initializeAtomicHistLoad.inputDFNameFromExecutionOrder matches "addFileDate" )
    assert(initializeAtomicHistLoad.insertColumns.map(_.trim).mkString(",") == "CompanyName,AccountId,CampaignName,CampaignId,CampaignBudget,CampaignStartDate,CampaignEndDate,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth,Revenue,filename")
    assert(initializeAtomicHistLoad.partitionColumns.map(_.trim).mkString(",") == "file_date")

    assert(initializeAtomicHistLoad.repartition.toString == "true")

  }


  it should "validate FndLoad case class " in {

    val initializeFndLoad = FndLoad(testConfig)

    assert(initializeFndLoad.inputDFNameFromExecutionOrder matches "rename" )
    assert(initializeFndLoad.insertColumns.map(_.trim).mkString(",") == "company_name,account_id,campaign_name,campaign_id,campaign_budget_a,campaign_start_date,campaign_end_date,class_id,dept_id,vendor_number,tcin,revenue_a,calendar_year,calendar_month")
    assert(initializeFndLoad.partitionColumns.map(_.trim).mkString(",") == "calendar_year,calendar_month")

    assert(initializeFndLoad.repartition.toString == "true")

  }

  it should "validate DeleteTriggerFile case class " in {

    val initializeDeleteTriggerFile = DeleteTriggerFile(testConfig)

    assert(initializeDeleteTriggerFile.doneFile matches "")
  }


  it should "test ShowResultsToLog case classes type" in {

    val initializeShowResultsToLog = Try(ShowResultsToLog(testConfig))

    val initializeShowResultsToLogResult = initializeShowResultsToLog.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeShowResultsToLogResult)
  }



  it should "test ShowCustomResultsToLog case classes type" in {
    val initializeShowCustomResultsToLog = Try(ShowCustomResultsToLog(testConfig))

    val initializeShowCustomResultsToLogResult = initializeShowCustomResultsToLog.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeShowCustomResultsToLogResult)
  }

  it should "test ExecutionOrder case classes type" in {
    val initializeExecutionOrder = Try(ExecutionOrder(testConfig))

    val initializeExecutionOrderResult = initializeExecutionOrder.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeExecutionOrderResult)
  }

  it should "test CommonPipeLineHelper case classes type" in {
    val initializeCommonExecuteParaMeterObjects = Try(CommonPipeLineHelper(testConfig))

    val initializeCommonExecuteParaMeterObjectsResult = initializeCommonExecuteParaMeterObjects.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeCommonExecuteParaMeterObjectsResult)
  }

  it should "test ReadInput case classes type" in {
    val initializeReadInput = Try(ReadInput(testConfig))

    val initializeReadInputResult = initializeReadInput.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeReadInputResult)
  }

  it should "test ValidateSchema case classes type" in {
    val initializeValidateSchema = Try(ValidateSchema(testConfig))

    val initializeValidateSchemaResult = initializeValidateSchema.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeValidateSchemaResult)
  }

  it should "test AddFileDate case classes type" in {
    val initializeAddFileDate = Try(AddFileDate(testConfig))

    val initializeAddFileDateResult = initializeAddFileDate.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeAddFileDateResult)
  }

  it should "test AddDatePartitionColumn case classes type" in {
    val initializeAddDatePartitionColumn = Try(AddDatePartitionColumn(testConfig))

    val initializeAddDatePartitionColumnResult = initializeAddDatePartitionColumn.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeAddDatePartitionColumnResult)
  }

  it should "test GetLatestByKeyCol case classes type" in {
    val initializeGetLatestByKeyCol = Try(GetLatestByKeyCol(testConfig))

    val initializeGetLatestByKeyColResult = initializeGetLatestByKeyCol.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeGetLatestByKeyColResult)
  }

  it should "test DropDups case classes type" in {
    val initializeDropDups = Try(DropDups(testConfig))

    val initializeDropDupsResult = initializeDropDups.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeDropDupsResult)
  }

  it should "test ValidateNotNull case classes type" in {

    val initializeValidateNotNull = Try(ValidateNotNull(testConfig))

    val initializeValidateNotNullResult = initializeValidateNotNull.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeValidateNotNullResult)
  }

  it should "test ValidateDataType case classes type" in {
    val initializeValidateDataType = Try(ValidateDataType(testConfig))

    val initializeValidateDataTypeResult = initializeValidateDataType.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeValidateDataTypeResult)
  }

  it should "test ValidateDateFormat case classes type" in {
    val initializeValidateDateFormat = Try(ValidateDateFormat(testConfig))

    val initializeValidateDateFormatResult = initializeValidateDateFormat.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeValidateDateFormatResult)
  }

  it should "test RemoveDollarCharacter case classes type" in {
    val initializeRemoveDollarCharacter = Try(RemoveDollarCharacter(testConfig))

    val initializeRemoveDollarCharacterResult = initializeRemoveDollarCharacter.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeRemoveDollarCharacterResult)
  }

  it should "test RemoveNonUtfCharacter case classes type" in {
    val initializeRemoveNonUtfCharacter = Try(RemoveNonUtfCharacter(testConfig))

    val initializeRemoveNonUtfCharacterResult = initializeRemoveNonUtfCharacter.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeRemoveNonUtfCharacterResult)
  }

  it should "test ReplaceNullWithDefaultForQtyColumn case classes type" in {
    val initializeReplaceNullWithDefaultForQtyColumn = Try(ReplaceNullWithDefaultForQtyColumn(testConfig))

    val initializeReplaceNullWithDefaultForQtyColumnResult = initializeReplaceNullWithDefaultForQtyColumn.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeReplaceNullWithDefaultForQtyColumnResult)
  }

  it should "test ReplaceNullWithDefaultForAmtColumn case classes type" in {
    val initializeReplaceNullWithDefaultForAmtColumn = Try(ReplaceNullWithDefaultForAmtColumn(testConfig))

    val initializeReplaceNullWithDefaultForAmtColumnResult = initializeReplaceNullWithDefaultForAmtColumn.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeReplaceNullWithDefaultForAmtColumnResult)
  }

  it should "test ReplaceNullWithDefaultForStringColumn case classes type" in {
    val initializeReplaceNullWithDefaultForStringColumn = Try(ReplaceNullWithDefaultForStringColumn(testConfig))

    val initializeReplaceNullWithDefaultForStringColumnResult = initializeReplaceNullWithDefaultForStringColumn.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeReplaceNullWithDefaultForStringColumnResult)
  }

  it should "test ConvertDateFormat case classes type" in {
    val initializeConvertDateFormat = Try(ConvertDateFormat(testConfig))

    val initializeConvertDateFormatResult = initializeConvertDateFormat.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeConvertDateFormatResult)
  }

  it should "test ConvertTimestampToDate case classes type" in {
    val initializeConvertTimestampToDate = Try(ConvertTimestampToDate(testConfig))

    val initializeConvertTimestampToDateResult = initializeConvertTimestampToDate.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeConvertTimestampToDateResult)
  }

  it should "test ConvertTimestampFromLocalToUtc case classes type" in {
    val initializeConvertTimestampFromLocalToUtc = Try(ConvertTimestampFromLocalToUtc(testConfig))

    val initializeConvertTimestampFromLocalToUtcResult = initializeConvertTimestampFromLocalToUtc.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeConvertTimestampFromLocalToUtcResult)
  }

  it should "test ConvertTimestampFromUtcToLocal case classes type" in {
    val initializeConvertTimestampFromUtcToLocal = Try(ConvertTimestampFromUtcToLocal(testConfig))

    val initializeConvertTimestampFromUtcToLocalResult = initializeConvertTimestampFromUtcToLocal.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeConvertTimestampFromUtcToLocalResult)
  }

  it should "test JoinCal case classes type" in {
    val initializeJoinCal = Try(JoinCal(testConfig))

    val initializeJoinCalResult = initializeJoinCal.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeJoinCalResult)
  }

  it should "test Rename case classes type" in {
    val initializeRename = Try(Rename(testConfig))

    val initializeRenameResult = initializeRename.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeRenameResult)
  }

  it should "test DqCheck case classes type" in {
    val initializeDqCheck = Try(DqCheck(testConfig))

    val initializeDqCheckResult = initializeDqCheck.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeDqCheckResult)
  }

  it should "test ArchiveArchivePartition case classes type" in {
    val initializeArchiveArchivePartition = Try(ArchiveArchivePartition(testConfig))

    val initializeArchiveArchivePartitionResult = initializeArchiveArchivePartition.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeArchiveArchivePartitionResult)

  }

  it should "test ArchiveEphemeral case classes type" in {
    val initializeArchiveEphemeral = Try(ArchiveEphemeral(testConfig))

    val initializeArchiveEphemeralResult = initializeArchiveEphemeral.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeArchiveEphemeralResult)
  }

  it should "test AtomicHistLoad case classes type" in {
    val initializeAtomicHistLoad = Try(AtomicHistLoad(testConfig))

    val initializeAtomicHistLoadResult = initializeAtomicHistLoad.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeAtomicHistLoadResult)
  }

  it should "test FndLoad case classes type" in {
    val initializeFndLoad = Try(FndLoad(testConfig))

    val initializeFndLoadResult = initializeFndLoad.isFailure match {
      case false => false
      case _ => true
    }
    assertResult(false)(initializeFndLoadResult)
  }



  }
