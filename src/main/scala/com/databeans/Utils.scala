package com.databeans

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, lit}

object Utils {
def joinHistoryAndUpdate (history : DataFrame, update : DataFrame) : DataFrame = {
  val updateWithNewSchema = update
    .withColumn("moved_out", lit(null))
  val newHistory = history.drop("current")

  val joinedHistoryAndUpdate = newHistory.union(updateWithNewSchema)
  val windowSpec = Window.partitionBy("id").orderBy("id")
  joinedHistoryAndUpdate.sort("moved_in", "moved_out")
    .withColumn("lead", lead("moved_in", 1) over windowSpec)
}
  def getTouchedHistory (rowsBeforeUpdates : DataFrame) : DataFrame = {
    rowsBeforeUpdates
      .where((col("moved_out").isNull) || (col("lead") < col("moved_out")))
  }
  def ModifyDates (touchedHistory : DataFrame) : DataFrame = {
    touchedHistory
      .withColumn("moved_out", col("lead"))
      .drop("lead")
  }
  def getTangentIntervalsOnSameAddress(rowsWithCorrectedMovedOut : DataFrame) : DataFrame = {
    val windowSpec = Window.partitionBy("id").orderBy("id")
    rowsWithCorrectedMovedOut.sort("moved_in", "moved_out")
      .withColumn("lead", lead("moved_in", 1) over windowSpec)
      .withColumn("new_address", lead("address", 1) over windowSpec)
      .withColumn("new_moved_out", lead("moved_out", 1) over windowSpec)
      .where((col("lead") === col("moved_out")) && (col("address") === col("new_address")))
  }
  def unifyTangentIntervals(rowsWithTangentIntervalsOnSameAddress : DataFrame): DataFrame ={
    rowsWithTangentIntervalsOnSameAddress
      .withColumn("moved_out", col("new_moved_out"))
      .drop("new_moved_out", "lead", "new_address")
  }
  def removeSeparatedTangentIntervals(rowsWithTangentIntervalsOnSameAddress: DataFrame): DataFrame = {
    val rowsWithTangentIntervalsRemoved = rowsWithTangentIntervalsOnSameAddress
      .drop("new_moved_out", "lead", "new_address")
    val rowsWithTangentToBeRemoved = rowsWithTangentIntervalsOnSameAddress
      .withColumn("address", col("new_address"))
      .withColumn("moved_in", col("lead"))
      .withColumn("moved_out", col("new_moved_out"))
      .drop("new_moved_out", "lead", "new_address")
    rowsWithTangentIntervalsRemoved.union(rowsWithTangentToBeRemoved)
  }
  def getUntouchedHistory (rowsBeforeUpdates: DataFrame): DataFrame = {
    rowsBeforeUpdates
      .where(col("lead") >= col("moved_out"))
      .drop("lead")
  }
  def addCurrentColumnToCorrectedDates(correctedDates : DataFrame):DataFrame = {
    val currents = correctedDates
      .where(col("moved_out").isNull)
      .withColumn("current", lit(true))
    val nonCurrents = correctedDates
      .where(col("moved_out").isNotNull)
      .withColumn("current", lit(false))
    currents.union(nonCurrents)
  }

}
