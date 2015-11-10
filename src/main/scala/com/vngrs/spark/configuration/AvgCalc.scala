package com.vngrs.spark

import java.io.File

import com.typesafe.config.ConfigFactory
import com.vngrs.common.Constants
import com.vngrs.spark.configuration.Config._
import com.vngrs.model.DailyWeather
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

/**
 * Created by sametozcan on 09/11/15.
 */
object AvgCalc {

  val properties = ConfigFactory.load("application.properties")
  private val logger = Logger.getLogger(getClass)

  /**
   * Calculates monthly average min, max and mean values for the months between 2012-02 and 2014-06
   */
  def run(): Unit = {

      val sc = connectToSparkCluster

      val delimiter = Constants.DELIMITER

      // Date part is changed containing only year and month for the operation
      val data = sc.textFile(properties.getString("file.path")).map(row => {
        val arr = row.split(delimiter)
        DailyWeather(arr(0).substring(0,7), arr(1).toShort, arr(2).toShort, arr(3).toShort)
      })

      val filteredData = data.filter(weather => weather.date >= "2012-12" && weather.date <= "2014-06")
      // Data is grouped monthly
      val grouped = filteredData.groupBy(weather => weather.date)
      val targetFolder = properties.getString("result.folder.path")

      // deletes if a file exists before writing to the file
      FileUtils.deleteQuietly(new File(targetFolder))

      grouped.map{case (date, weatherReports) => {

        val numberOfDays = weatherReports.size
        val avgMin = weatherReports.map(_.min).sum.toDouble/numberOfDays
        val avgMax = weatherReports.map(_.max).sum.toDouble/numberOfDays
        val avgMean = weatherReports.map(_.mean).sum.toDouble/numberOfDays
        date + delimiter + avgMin + delimiter + avgMax + delimiter + avgMean

      }
      }.saveAsTextFile(targetFolder)

      logger.info("Average weather calculations ended successfully!")
      logger.info("Results are written to the folder " + targetFolder)

      sc.stop

    }

}
