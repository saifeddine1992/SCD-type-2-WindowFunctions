package com.databeans

import com.databeans.Utils._
import org.apache.spark.sql.DataFrame

object AddressHistoryBuilder {
  def buildHistory(history: DataFrame, update: DataFrame): DataFrame = {


    val rowsBeforeUpdates = joinHistoryAndUpdate(history, update)
    val touchedHistory = getTouchedHistory(rowsBeforeUpdates)

    val rowsWithCorrectedMovedOut = ModifyDates(touchedHistory)
    val rowsWithTangentIntervalsOnSameAddress = getTangentIntervalsOnSameAddress(rowsWithCorrectedMovedOut)
    val unifiedTangentIntervals = unifyTangentIntervals(rowsWithTangentIntervalsOnSameAddress)
    val separatedTangentIntervals = removeSeparatedTangentIntervals(rowsWithTangentIntervalsOnSameAddress)
    val untouchedHistory = getUntouchedHistory(rowsBeforeUpdates)


    val correctedDates = rowsWithCorrectedMovedOut
      .except(separatedTangentIntervals)
      .union(unifiedTangentIntervals)
      .union(untouchedHistory)

    addCurrentColumnToCorrectedDates(correctedDates)

  }
}
